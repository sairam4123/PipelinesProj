package main

import (
	"context"
	"fmt"
	"time"
)

type Result struct {
	Success bool
	Error   error
	Data    any
}

type Intermediate struct {
	lastStepIdx int
	Steps       []*Step
}

type Step struct {
	Step  string
	In    any
	Out   any
	Error error
}

type Request struct {
	Data any
}

func generator(ctx context.Context, reqs []*Request) <-chan *Intermediate {
	out := make(chan *Intermediate, 10)
	go func() {
		defer close(out)
		for _, req := range reqs {
			fmt.Println("Generating req", req.Data)
			o := &Step{Step: "gen", Out: req.Data, In: nil, Error: nil}
			steps := make([]*Step, 0)
			time.Sleep(500 * time.Millisecond)
			inter := &Intermediate{lastStepIdx: 0, Steps: append(steps, o)}
			select {
			case <-ctx.Done():
				return
			case out <- inter:
			}
		}
	}()
	return out
}

// expects integer as input outputs integer
func double(ctx context.Context, in <-chan *Intermediate) <-chan *Intermediate {

	out := make(chan *Intermediate, 10)
	go func() {
		defer close(out)
		for d := range in {
			inp := d.Steps[d.lastStepIdx].Out.(int)
			step := &Step{In: inp, Out: inp + inp, Step: "double", Error: nil}
			o := &Intermediate{Steps: append(d.Steps, step), lastStepIdx: d.lastStepIdx + 1}
			fmt.Println("Processing step", "double", step.Out)
			if inp%3 == 0 {
				time.Sleep(1 * time.Second)
			}
			select {
			case <-ctx.Done():
				fmt.Printf("Cancelling...")
				return
			case out <- o:
				fmt.Printf("Successfully sent %#v\n", step.Out)
			}
		}
	}()
	return out
}

func collect(in <-chan *Intermediate) []*Result {
	result := make([]*Result, 0)
	for inter := range in {
		out := inter.Steps[inter.lastStepIdx]
		fmt.Println("Collecting output... ", out.Out)
		result = append(result, &Result{
			Success: out.Error == nil,
			Error:   out.Error,
			Data:    out.Out,
		})
	}
	return result
}

func main() {

	reqs := make([]*Request, 0)
	for i := range 100 {
		reqs = append(reqs, &Request{
			Data: i,
		})
		fmt.Println("Creating request ", i)
	}
	fmt.Println(reqs)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	ch0 := generator(ctx, reqs)
	ch1 := double(ctx, ch0)
	ch2 := double(ctx, ch1)
	res := collect(ch2)
	for _, result := range res {
		fmt.Println(result)
	}

}
