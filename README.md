# bun.go

A no-nonsense ("bring your own toppings") framework for building services on top of NATS. Inspired by popular HTTP frameworks
[echo](https://github.com/labstack/echo) and [gin](https://github.com/gin-gonic/gin).

## Installation

```
go get github.com/dnbsd/bun.go
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

### Connection event handling

```go
import "github.com/dnbsd/bun.go"

b := bun.New(bun.Arguments{
    Servers: []string{
        "localhost",
    }
})

b.ConnectedHandler = func(nc *nats.Conn) {
    println("connected")
}
b.ReconnectedHandler = func(nc *nats.Conn) {
    println("reconnected")
}
b.DisconnectedHandler = func(nc *nats.Conn, err error) {
    println("disconnected")
}
```

### Stream handling

```
JetStream support is currently a work in progress.
```

## License

MIT