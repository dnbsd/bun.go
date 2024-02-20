# bun

A framework for building services on top of NATS.

## Installation

```
$ go get github.com/dnbsd/bun.go
```

## Usage

### Request/Reply handling

```go
import "github.com/dnbsd/bun.go"

b := bun.New(bun.Arguments{
    Servers: []string{
        "localhost",
    }
})

b.Subscribe("rpc.users.create", func(c *bun.Context){
    type request struct {
        Username string `json:"username"`
        Password string `json:"password"`
    }
    type response struct {
        ID uint64 `json:"id"`
    }

    var r request
    if err := c.BindJSON(&r); err != nil {
        return c.Error(err)
    }

    //
    // Do things with request
    //

    return c.JSON(response{
        ID: 1,
    })
})
```

## License

TODO