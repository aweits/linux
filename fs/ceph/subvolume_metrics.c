// SPDX-License-Identifier: GPL-2.0
#include <linux/ceph/ceph_debug.h>

#include <linux/math64.h>
#include <linux/slab.h>
#include <linux/seq_file.h>

#include "subvolume_metrics.h"
#include "mds_client.h"
#include "super.h"

/**
 * struct ceph_subvol_metric_rb_entry - Per-subvolume I/O metrics node
 * @node: Red-black tree linkage for tracker->tree
 * @subvolume_id: Subvolume identifier (key for rb-tree lookup)
 * @read_ops: Accumulated read operation count since last snapshot
 * @write_ops: Accumulated write operation count since last snapshot
 * @read_bytes: Accumulated bytes read since last snapshot
 * @write_bytes: Accumulated bytes written since last snapshot
 * @read_latency_us: Sum of read latencies in microseconds
 * @write_latency_us: Sum of write latencies in microseconds
 */
struct ceph_subvol_metric_rb_entry {
	struct rb_node node;
	u64 subvolume_id;
	u64 read_ops;
	u64 write_ops;
	u64 read_bytes;
	u64 write_bytes;
	u64 read_latency_us;
	u64 write_latency_us;
};

/**
 * struct ceph_subvol_metric_pcpu - Per-CPU accumulator in front of the tree
 * @lock: Protects @m; only contended when a task migrates mid-update
 * @m: Counters of one subvolume (@m.subvolume_id) not yet folded into the tree
 *
 * I/O completions add to the slot of the CPU they run on and only take
 * tracker->lock when the slot holds another subvolume, or when a snapshot
 * or a dump folds every slot into the tree.
 */
struct ceph_subvol_metric_pcpu {
	spinlock_t lock;
	struct ceph_subvol_metric_snapshot m;
};

static struct kmem_cache *ceph_subvol_metric_entry_cachep;

int ceph_subvolume_metrics_init(struct ceph_subvolume_metrics_tracker *tracker)
{
	int cpu;

	spin_lock_init(&tracker->lock);
	tracker->tree = RB_ROOT_CACHED;
	tracker->nr_entries = 0;
	tracker->enabled = false;
	tracker->pcpu = alloc_percpu(struct ceph_subvol_metric_pcpu);
	if (!tracker->pcpu)
		return -ENOMEM;
	for_each_possible_cpu(cpu)
		spin_lock_init(&per_cpu_ptr(tracker->pcpu, cpu)->lock);
	return 0;
}

static struct ceph_subvol_metric_rb_entry *
__lookup_entry(struct ceph_subvolume_metrics_tracker *tracker, u64 subvol_id)
{
	struct rb_node *node;

	node = tracker->tree.rb_root.rb_node;
	while (node) {
		struct ceph_subvol_metric_rb_entry *entry =
			rb_entry(node, struct ceph_subvol_metric_rb_entry, node);

		if (subvol_id < entry->subvolume_id)
			node = node->rb_left;
		else if (subvol_id > entry->subvolume_id)
			node = node->rb_right;
		else
			return entry;
	}

	return NULL;
}

static struct ceph_subvol_metric_rb_entry *
__insert_entry(struct ceph_subvolume_metrics_tracker *tracker,
	       struct ceph_subvol_metric_rb_entry *entry)
{
	struct rb_node **link = &tracker->tree.rb_root.rb_node;
	struct rb_node *parent = NULL;
	bool leftmost = true;

	while (*link) {
		struct ceph_subvol_metric_rb_entry *cur =
			rb_entry(*link, struct ceph_subvol_metric_rb_entry, node);

		parent = *link;
		if (entry->subvolume_id < cur->subvolume_id)
			link = &(*link)->rb_left;
		else if (entry->subvolume_id > cur->subvolume_id) {
			link = &(*link)->rb_right;
			leftmost = false;
		} else
			return cur;
	}

	rb_link_node(&entry->node, parent, link);
	rb_insert_color_cached(&entry->node, &tracker->tree, leftmost);
	tracker->nr_entries++;
	return entry;
}

static void ceph_subvolume_metrics_clear_locked(
		struct ceph_subvolume_metrics_tracker *tracker)
{
	struct rb_node *node = rb_first_cached(&tracker->tree);

	while (node) {
		struct ceph_subvol_metric_rb_entry *entry =
			rb_entry(node, struct ceph_subvol_metric_rb_entry, node);
		struct rb_node *next = rb_next(node);

		rb_erase_cached(&entry->node, &tracker->tree);
		tracker->nr_entries--;
		kmem_cache_free(ceph_subvol_metric_entry_cachep, entry);
		node = next;
	}

	tracker->tree = RB_ROOT_CACHED;
}

static void ceph_subvolume_metrics_clear_pcpu(
		struct ceph_subvolume_metrics_tracker *tracker)
{
	int cpu;

	for_each_possible_cpu(cpu) {
		struct ceph_subvol_metric_pcpu *pc = per_cpu_ptr(tracker->pcpu, cpu);

		spin_lock(&pc->lock);
		memset(&pc->m, 0, sizeof(pc->m));
		spin_unlock(&pc->lock);
	}
}

void ceph_subvolume_metrics_destroy(struct ceph_subvolume_metrics_tracker *tracker)
{
	spin_lock(&tracker->lock);
	ceph_subvolume_metrics_clear_locked(tracker);
	tracker->enabled = false;
	spin_unlock(&tracker->lock);
	free_percpu(tracker->pcpu);
	tracker->pcpu = NULL;
}

void ceph_subvolume_metrics_enable(struct ceph_subvolume_metrics_tracker *tracker,
				   bool enable)
{
	spin_lock(&tracker->lock);
	if (enable) {
		tracker->enabled = true;
	} else {
		tracker->enabled = false;
		ceph_subvolume_metrics_clear_locked(tracker);
	}
	spin_unlock(&tracker->lock);
	if (!enable)
		ceph_subvolume_metrics_clear_pcpu(tracker);
}

/*
 * Add the counters of one subvolume taken out of a per-CPU slot to its
 * entry in the tree.
 */
static void ceph_subvolume_metrics_fold(struct ceph_subvolume_metrics_tracker *tracker,
					const struct ceph_subvol_metric_snapshot *d)
{
	struct ceph_subvol_metric_rb_entry *entry, *new_entry = NULL;
	bool retry = false;

	/*
	 * Retry loop for lock-free allocation pattern:
	 * 1. First iteration: lookup under lock, if miss -> drop lock, alloc, retry
	 * 2. Second iteration: lookup again (may have been inserted), insert if still missing
	 * 3. On race (another thread inserted same key): free our alloc, retry
	 * All successful paths exit via return, so retry flag doesn't need reset.
	 */
	do {
		spin_lock(&tracker->lock);
		if (!tracker->enabled) {
			spin_unlock(&tracker->lock);
			if (new_entry)
				kmem_cache_free(ceph_subvol_metric_entry_cachep, new_entry);
			return;
		}

		entry = __lookup_entry(tracker, d->subvolume_id);
		if (!entry) {
			if (!new_entry) {
				spin_unlock(&tracker->lock);
				new_entry = kmem_cache_zalloc(ceph_subvol_metric_entry_cachep,
						      GFP_NOFS);
				if (!new_entry)
					return;
				new_entry->subvolume_id = d->subvolume_id;
				retry = true;
				continue;
			}
			entry = __insert_entry(tracker, new_entry);
			if (entry != new_entry) {
				/* raced with another insert */
				spin_unlock(&tracker->lock);
				kmem_cache_free(ceph_subvol_metric_entry_cachep, new_entry);
				new_entry = NULL;
				retry = true;
				continue;
			}
			new_entry = NULL;
		}

		entry->read_ops += d->read_ops;
		entry->read_bytes += d->read_bytes;
		entry->read_latency_us += d->read_latency_us;
		entry->write_ops += d->write_ops;
		entry->write_bytes += d->write_bytes;
		entry->write_latency_us += d->write_latency_us;
		spin_unlock(&tracker->lock);
		if (new_entry)
			kmem_cache_free(ceph_subvol_metric_entry_cachep, new_entry);
		return;
	} while (retry);
}

/* Move what every per-CPU slot holds into the tree. */
static void ceph_subvolume_metrics_fold_pcpu(
		struct ceph_subvolume_metrics_tracker *tracker)
{
	int cpu;

	for_each_possible_cpu(cpu) {
		struct ceph_subvol_metric_pcpu *pc = per_cpu_ptr(tracker->pcpu, cpu);
		struct ceph_subvol_metric_snapshot d;

		spin_lock(&pc->lock);
		d = pc->m;
		memset(&pc->m, 0, sizeof(pc->m));
		pc->m.subvolume_id = d.subvolume_id;
		spin_unlock(&pc->lock);

		if (d.read_ops || d.write_ops)
			ceph_subvolume_metrics_fold(tracker, &d);
	}
}

void ceph_subvolume_metrics_record(struct ceph_subvolume_metrics_tracker *tracker,
				   u64 subvol_id, bool is_write,
				   size_t size, u64 latency_us)
{
	struct ceph_subvol_metric_snapshot old = {};
	struct ceph_subvol_metric_pcpu *pc;

	/* CEPH_SUBVOLUME_ID_NONE (0) means unknown/unset subvolume */
	if (!READ_ONCE(tracker->enabled) ||
	    subvol_id == CEPH_SUBVOLUME_ID_NONE || !size || !latency_us)
		return;

	/*
	 * The slot of the CPU we start on; if we migrate, the lock still
	 * keeps the update consistent and it just lands in another slot.
	 */
	pc = raw_cpu_ptr(tracker->pcpu);
	spin_lock(&pc->lock);
	if (unlikely(pc->m.subvolume_id != subvol_id)) {
		old = pc->m;
		memset(&pc->m, 0, sizeof(pc->m));
		pc->m.subvolume_id = subvol_id;
	}
	if (is_write) {
		pc->m.write_ops++;
		pc->m.write_bytes += size;
		pc->m.write_latency_us += latency_us;
	} else {
		pc->m.read_ops++;
		pc->m.read_bytes += size;
		pc->m.read_latency_us += latency_us;
	}
	spin_unlock(&pc->lock);

	/* the slot held another subvolume: hand its counters to the tree */
	if (old.read_ops || old.write_ops)
		ceph_subvolume_metrics_fold(tracker, &old);
}

int ceph_subvolume_metrics_snapshot(struct ceph_subvolume_metrics_tracker *tracker,
				    struct ceph_subvol_metric_snapshot **out,
				    u32 *nr, bool consume)
{
	struct ceph_subvol_metric_snapshot *snap = NULL;
	struct rb_node *node;
	u32 count = 0, idx = 0;
	int ret = 0;

	*out = NULL;
	*nr = 0;

	if (!READ_ONCE(tracker->enabled))
		return 0;

	ceph_subvolume_metrics_fold_pcpu(tracker);

	spin_lock(&tracker->lock);
	for (node = rb_first_cached(&tracker->tree); node; node = rb_next(node)) {
		struct ceph_subvol_metric_rb_entry *entry =
			rb_entry(node, struct ceph_subvol_metric_rb_entry, node);

		/* Include entries with ANY I/O activity (read OR write) */
		if (entry->read_ops || entry->write_ops)
			count++;
	}
	spin_unlock(&tracker->lock);

	if (!count)
		return 0;

	snap = kzalloc_objs(*snap, count, GFP_NOFS);
	if (!snap)
		return -ENOMEM;

	spin_lock(&tracker->lock);
	node = rb_first_cached(&tracker->tree);
	while (node) {
		struct ceph_subvol_metric_rb_entry *entry =
			rb_entry(node, struct ceph_subvol_metric_rb_entry, node);
		struct rb_node *next = rb_next(node);

		/* Skip entries with NO I/O activity at all */
		if (!entry->read_ops && !entry->write_ops) {
			rb_erase_cached(&entry->node, &tracker->tree);
			tracker->nr_entries--;
			kmem_cache_free(ceph_subvol_metric_entry_cachep, entry);
			node = next;
			continue;
		}

		if (idx >= count) {
			pr_warn("ceph: subvol metrics snapshot race (idx=%u count=%u)\n",
				idx, count);
			break;
		}

		snap[idx].subvolume_id = entry->subvolume_id;
		snap[idx].read_ops = entry->read_ops;
		snap[idx].write_ops = entry->write_ops;
		snap[idx].read_bytes = entry->read_bytes;
		snap[idx].write_bytes = entry->write_bytes;
		snap[idx].read_latency_us = entry->read_latency_us;
		snap[idx].write_latency_us = entry->write_latency_us;
		idx++;

		if (consume) {
			entry->read_ops = 0;
			entry->write_ops = 0;
			entry->read_bytes = 0;
			entry->write_bytes = 0;
			entry->read_latency_us = 0;
			entry->write_latency_us = 0;
			rb_erase_cached(&entry->node, &tracker->tree);
			tracker->nr_entries--;
			kmem_cache_free(ceph_subvol_metric_entry_cachep, entry);
		}
		node = next;
	}
	spin_unlock(&tracker->lock);

	if (!idx) {
		kfree(snap);
		snap = NULL;
		ret = 0;
	} else {
		*nr = idx;
		*out = snap;
	}

	return ret;
}

void ceph_subvolume_metrics_free_snapshot(struct ceph_subvol_metric_snapshot *snapshot)
{
	kfree(snapshot);
}

/*
 * Dump subvolume metrics to a seq_file for debugfs.
 *
 * Iterates the rb-tree directly under spinlock to avoid allocation.
 * The lock hold time is minimal since we're only doing seq_printf calls.
 */
void ceph_subvolume_metrics_dump(struct ceph_subvolume_metrics_tracker *tracker,
				 struct seq_file *s)
{
	struct rb_node *node;
	bool found = false;

	if (READ_ONCE(tracker->enabled))
		ceph_subvolume_metrics_fold_pcpu(tracker);

	spin_lock(&tracker->lock);
	if (!tracker->enabled) {
		spin_unlock(&tracker->lock);
		seq_puts(s, "subvolume metrics disabled\n");
		return;
	}

	for (node = rb_first_cached(&tracker->tree); node; node = rb_next(node)) {
		struct ceph_subvol_metric_rb_entry *entry =
			rb_entry(node, struct ceph_subvol_metric_rb_entry, node);
		u64 avg_rd_lat, avg_wr_lat;

		if (!entry->read_ops && !entry->write_ops)
			continue;

		if (!found) {
			seq_puts(s, "subvol_id       rd_ops    rd_bytes    rd_avg_lat_us  wr_ops    wr_bytes    wr_avg_lat_us\n");
			seq_puts(s, "------------------------------------------------------------------------------------------------\n");
			found = true;
		}

		avg_rd_lat = entry->read_ops ?
			div64_u64(entry->read_latency_us, entry->read_ops) : 0;
		avg_wr_lat = entry->write_ops ?
			div64_u64(entry->write_latency_us, entry->write_ops) : 0;

		seq_printf(s, "%-15llu%-10llu%-12llu%-16llu%-10llu%-12llu%-16llu\n",
			   entry->subvolume_id,
			   entry->read_ops,
			   entry->read_bytes,
			   avg_rd_lat,
			   entry->write_ops,
			   entry->write_bytes,
			   avg_wr_lat);
	}
	spin_unlock(&tracker->lock);

	if (!found)
		seq_puts(s, "(no subvolume metrics collected)\n");
}

void ceph_subvolume_metrics_record_io(struct ceph_mds_client *mdsc,
				      struct ceph_inode_info *ci,
				      bool is_write, size_t bytes,
				      ktime_t start, ktime_t end)
{
	struct ceph_subvolume_metrics_tracker *tracker;
	u64 subvol_id;
	s64 delta_us;

	if (!mdsc || !ci || !bytes)
		return;

	tracker = &mdsc->subvol_metrics;
	if (!ceph_subvolume_metrics_enabled(tracker))
		return;

	subvol_id = READ_ONCE(ci->i_subvolume_id);
	if (subvol_id == CEPH_SUBVOLUME_ID_NONE)
		return;

	delta_us = ktime_to_us(ktime_sub(end, start));
	if (delta_us <= 0)
		delta_us = 1;

	ceph_subvolume_metrics_record(tracker, subvol_id, is_write,
				      bytes, (u64)delta_us);
}

int __init ceph_subvolume_metrics_cache_init(void)
{
	ceph_subvol_metric_entry_cachep = KMEM_CACHE(ceph_subvol_metric_rb_entry,
						    SLAB_RECLAIM_ACCOUNT);
	if (!ceph_subvol_metric_entry_cachep)
		return -ENOMEM;
	return 0;
}

void ceph_subvolume_metrics_cache_destroy(void)
{
	kmem_cache_destroy(ceph_subvol_metric_entry_cachep);
}
