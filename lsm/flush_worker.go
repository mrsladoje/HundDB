package lsm

import (
	"fmt"
	memtable "hunddb/lsm/memtable"
	"sync"
)

// flushJob represents a single memtable flush task with a pre-assigned SSTable index
type flushJob struct {
	pos   int                // position in batch (0 = oldest)
	index int                // assigned SSTable index
	mt    *memtable.MemTable // memtable to flush
	resCh chan<- flushResult // channel to send the result
}

type flushResult struct {
	pos   int
	index int
	err   error
}

// FlushPool is a simple worker pool used to concurrently flush memtables
type FlushPool struct {
	jobs chan flushJob
	wg   sync.WaitGroup
}

// NewFlushPool creates a pool with the given worker count and starts workers immediately
func NewFlushPool(workerCount int) *FlushPool {
	p := &FlushPool{
		jobs: make(chan flushJob),
	}
	p.start(workerCount)
	return p
}

func (p *FlushPool) start(workerCount int) {
	for i := 0; i < workerCount; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for job := range p.jobs {
				// Perform the flush
				err := job.mt.Flush(job.index)
				job.resCh <- flushResult{pos: job.pos, index: job.index, err: err}
			}
		}()
	}
}

// Stop gracefully stops the pool; should be called on shutdown if needed
func (p *FlushPool) Stop() {
	close(p.jobs)
	p.wg.Wait()
}

// submitBatch submits a batch of flush jobs and commits results to level 0 in-order (oldest to newest)
func (p *FlushPool) submitBatch(lsm *LSM, memtables []*memtable.MemTable, indexes []int, lowWaterMarks []uint64) {
	n := len(memtables)
	resCh := make(chan flushResult, n)

	// Coordinator to ensure in-order commit (oldest->newest)
	go func() {
		defer close(resCh)
	}()

	// Collector and committer
	go func() {
		pending := make(map[int]flushResult, n)
		next := 0
		committed := 0
		for committed < n {
			r := <-resCh
			pending[r.pos] = r
			for {
				rr, ok := pending[next]
				if !ok {
					break
				}
				// Only append to levels when the specific position is done (ensures ordering)
				if rr.err == nil {
					// Commit to level 0 under its compaction lock to avoid race with compaction
					lsm.levelLocks[0].Lock()
					lsm.mu.Lock()
					lsm.levels[0] = append(lsm.levels[0], uint64(rr.index))

					// After successful flush, use the memtable's low water mark to clean up WAL segments
					// The low water mark for this memtable position tells us which WAL segments can be deleted
					if rr.pos < len(lowWaterMarks) {
						lowWaterMark := lowWaterMarks[rr.pos]
						if lowWaterMark > 0 {
							// Clean up WAL segments below this low water mark
							if err := lsm.wal.DeleteOldLogs(lowWaterMark); err != nil {
								// Log error but don't fail the flush
								fmt.Printf("Warning: Failed to delete old WAL logs below watermark %d: %v\n", lowWaterMark, err)
							}
						}
					}

					lsm.mu.Unlock()
					lsm.levelLocks[0].Unlock()

					// After successful append, consider compactions
					lsm.maybeStartCompactions()
				}
				delete(pending, next)
				next++
				committed++
			}
		}
	}()

	// Enqueue jobs in order (oldest first)
	for i := 0; i < n; i++ {
		p.jobs <- flushJob{pos: i, index: indexes[i], mt: memtables[i], resCh: resCh}
	}
}
