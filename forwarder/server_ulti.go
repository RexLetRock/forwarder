package forwarder

import (
	"bufio"
	"fmt"
	"time"
)

const BILL = 1_000_000_000

type PerformanceCounter struct {
	stime   int64
	ltime   int64
	ctime   int64
	step    int64
	nstep   int64
	counter int64
	prefix  string
}

func PerformanceCounterCreate(step int, timeToShow int, prefix string) *PerformanceCounter {
	ctime := now()
	step64 := int64(step)
	p := &PerformanceCounter{
		stime:   ctime,
		ltime:   ctime,
		ctime:   ctime,
		step:    step64,
		nstep:   step64,
		counter: 0,
		prefix:  prefix,
	}
	if timeToShow != 0 {
		time.AfterFunc(time.Duration(timeToShow*int(time.Second)), func() {
			p.Result()
		})
	}
	return p
}

// Count step default is hidden
func (s *PerformanceCounter) Step(show ...bool) {
	s.counter += 1
	if s.counter == 1 {
		s.ctime = now()
		s.ltime = s.ctime
		s.stime = s.ltime
	}

	if s.counter >= s.nstep {
		tmpTime := now()
		s.nstep += s.step
		if len(show) != 0 && show[0] {
			fmt.Printf("%v : %v opts in %vms, %v/sec, %v ns/op \n", s.prefix, commaize(s.counter), (tmpTime-s.stime)/int64(time.Millisecond), commaize(s.step*BILL/(tmpTime-s.ctime)), (tmpTime-s.stime)/s.counter)
		}
		s.ltime = s.ctime
		s.ctime = tmpTime
	}
}

func (s *PerformanceCounter) Result() {
	tmpTime := now()
	if s.ctime != s.stime {
		tmpTime = s.ctime
	}
	fmt.Printf("%v : %v opts in %vms, %v/sec, %v ns/op \n", s.prefix, commaize(s.counter), (tmpTime-s.stime)/int64(time.Millisecond), commaize(s.counter*BILL/(tmpTime-s.stime)), (tmpTime-s.stime)/s.counter)
}

func now() int64 {
	return time.Now().UnixNano()
}

func commaize(n int64) string {
	s1, s2 := fmt.Sprintf("%d", n), ""
	for i, j := len(s1)-1, 0; i >= 0; i, j = i-1, j+1 {
		if j%3 == 0 && j != 0 {
			s2 = "," + s2
		}
		s2 = string(s1[i]) + s2
	}
	return s2
}

func readWithEnd(reader *bufio.Reader) ([]byte, error) {
	message, err := reader.ReadBytes('#')
	if err != nil {
		return nil, err
	}

	a1, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	message = append(message, a1)
	if a1 != '\t' {
		message2, err := readWithEnd(reader)
		if err != nil {
			return nil, err
		}
		ret := append(message, message2...)
		return ret, nil
	}

	a2, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	message = append(message, a2)
	if a2 != '#' {
		message2, err := readWithEnd(reader)
		if err != nil {
			return nil, err
		}
		ret := append(message, message2...)
		return ret, nil
	}
	return message, nil
}
