package snowflake

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// Twitter_Snowflake
// SnowFlake的结构如下(每部分用-分开):
// 0 - 0000000000 0000000000 0000000000 0000000000 0 - 00000 - 00000 - 000000000000
// 1位标识，由于long基本类型在Java中是带符号的，最高位是符号位，正数是0，负数是1，所以id一般是正数，最高位是0
// 41位时间截(毫秒级)，注意，41位时间截不是存储当前时间的时间截，而是存储时间截的差值（当前时间截 - 开始时间截)
// 得到的值 这里的的开始时间截，一般是我们的id生成器开始使用的时间，由我们程序来指定的（如下下面程序IdWorker类的startTime属性）。41位的时间截，可以使用69年，年T = (1L << 41) / (1000L * 60 * 60 * 24 * 365) = 69
// 10位的数据机器位，可以部署在1024个节点，包括5位datacenterId和5位workerId
// 12位序列，毫秒内的计数，12位的计数顺序号支持每个节点每毫秒(同一机器，同一时间截)产生4096个ID序号
// 加起来刚好64位，为一个Long型
// SnowFlake的优点是，整体上按照时间自增排序，并且整个分布式系统内不会产生ID碰撞(由数据中心ID和机器ID作区分)，并且效率较高
const (
	twepoch = 1546272000000 // 默认起始的时间戳 1546272000000 (2019-01-01)

	workerIDBits     = 5                             //机器id所占的位数
	datacenterIDBits = 5                             //数据标识id所占的位数
	maxWorkerID      = -1 ^ (-1 << workerIDBits)     //支持的最大机器id，结果是31 (这个移位算法可以很快的计算出几位二进制数所能表示的最大十进制数)
	maxDatacenterID  = -1 ^ (-1 << datacenterIDBits) // 支持的最大数据标识id，结果是31
	sequenceBits     = 12                            //序列在id中占的位数

	workerIDShift      = sequenceBits                                   //机器ID向左移12位
	datacenterIDShift  = sequenceBits + workerIDBits                    //数据标识id向左移17位(12+5)
	timestampLeftShift = sequenceBits + workerIDBits + datacenterIDBits //时间截向左移22位(5+5+12)
	sequenceMask       = -1 ^ (-1 << sequenceBits)                      //生成序列的掩码，这里为4095 (0b111111111111=0xfff=4095)
)

type worker struct {
	workerID      int64
	datacenterID  int64
	sequence      int64
	lastTimestamp int64
	mutex         sync.Mutex
}

// Worker snowflake worker
type Worker interface {
	Next() (int64, error)
}

// NewWorker return new snowflake worker
func NewWorker(workerID uint8, datacenterID uint8) Worker {
	if workerID > maxWorkerID {
		log.Fatalf("worker Id can't be greater than %d or less than 0", maxWorkerID)
	}
	if datacenterID > maxDatacenterID {
		log.Fatalf("datacenter Id can't be greater than %d or less than 0", maxDatacenterID)
	}
	return &worker{workerID: int64(workerID), datacenterID: int64(datacenterID)}
}

func tilNextMillis(lastTimestamp int64) int64 {
	timestamp := time.Now().UnixNano() / 1e6
	for timestamp <= lastTimestamp {
		timestamp = time.Now().UnixNano() / 1e6
	}
	return timestamp
}

// Next return new id
func (w *worker) Next() (int64, error) {
	timestamp := time.Now().UnixNano() / 1e6
	if timestamp < w.lastTimestamp {
		return 0, fmt.Errorf("Clock moved backwards.  Refusing to generate id for %d milliseconds", w.lastTimestamp-timestamp)
	}
	w.mutex.Lock()
	if timestamp == w.lastTimestamp {
		w.sequence = (w.sequence + 1) & sequenceMask
		if w.sequence == 0 {
			// wait new timestamp
			timestamp = tilNextMillis(w.lastTimestamp)
		}
	} else {
		w.sequence = 0
	}
	w.lastTimestamp = timestamp
	id := ((timestamp - twepoch) << timestampLeftShift) |
		(w.datacenterID << datacenterIDShift) |
		(w.workerID << workerIDShift) |
		w.sequence
	w.mutex.Unlock()
	return id, nil
}

// DefaultWorker return @see NewWorker(0,0)
var DefaultWorker = &worker{}

// Next return DefaultWorker new id
func Next() (int64, error) {
	return DefaultWorker.Next()
}
