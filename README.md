# ChannelsProj

A small Go example demonstrating the **pipeline pattern** with channels and goroutines. Each stage of the pipeline runs concurrently, passing data to the next stage over an unbuffered channel.

## How it works

The pipeline is built from stages that each accept a `<-chan *Intermediate` and return a `<-chan *Intermediate`:

```
generator → double → double → collect
```

- **`generator`** takes a slice of `*Request` and emits one `*Intermediate` per request on its output channel.
- **`double`** reads each `*Intermediate`, doubles the integer produced by the previous step, and appends a new `*Step` recording the input, output, and any error.
- **`collect`** drains the final channel and converts each `*Intermediate` into a `*Result`.

Each `*Intermediate` carries its full history as a `[]*Step`, with `lastStepIdx` pointing at the most recently completed step — so the pipeline keeps a trace of every transformation a value went through, not just its final value.

In `main.go`, the pipeline is composed as:

```go
ch0 := generator(reqs)
ch1 := double(ch0)
ch2 := double(ch1)
res := collect(ch2)
```

This runs each request through two doubling stages (e.g. `3 → 6 → 12`), and prints the collected results.

## Requirements

- Go 1.26.3+

## Running

```bash
go run main.go
```

## Project layout

| File      | Purpose                                      |
|-----------|-----------------------------------------------|
| `main.go` | Pipeline stages (`generator`, `double`, `collect`) and `main` |
| `go.mod`  | Module definition (`channelsproj`)            |
