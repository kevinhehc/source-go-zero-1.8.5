package executors

import "time"

const defaultFlushInterval = time.Second

// Execute defines the method to execute tasks.
// executors 就是一个带缓冲、可定时/定量刷新的“批处理任务池”，用来把大量小任务攒成一批再统一执行，从而减少 I/O 次数、提升吞吐量。
type Execute func(tasks []any)
