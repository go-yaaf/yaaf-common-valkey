GO-YAAF Valkey Middleware
=================
![Project status](https://img.shields.io/badge/version-1.2-green.svg)
[![Build](https://github.com/go-yaaf/yaaf-common-valkey/actions/workflows/build.yml/badge.svg)](https://github.com/go-yaaf/yaaf-common-valkey/actions/workflows/build.yml)
[![Coverage Status](https://coveralls.io/repos/go-yaaf/yaaf-common-valkey/badge.svg?branch=main&service=github)](https://coveralls.io/github/go-yaaf/yaaf-common-valkey?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-yaaf/yaaf-common-valkey)](https://goreportcard.com/report/github.com/go-yaaf/yaaf-common-valkey)
[![GoDoc](https://godoc.org/github.com/go-yaaf/yaaf-common-valkey?status.svg)](https://pkg.go.dev/github.com/go-yaaf/yaaf-common-valkey)
![License](https://img.shields.io/dub/l/vibe-d.svg)


This library contains [Valkey](https://valkey.io) based implementation of the following middleware interfaces:
- The messaging patterns defined by the `IMessageBus` interface of the `yaaf-common` library.
- Distributed data cache defined by the `IDataCache` interface of the `yaaf-common` library.

Installation
------------

Use go get.

	go get -v -t github.com/go-yaaf/yaaf-common-valkey

Then import the validator package into your own code.

	import "github.com/go-yaaf/yaaf-common-valkey"


Usage
------------
Use Valkey wrapper using a connection string: 
* valkey://host:port/12
* valkey://host:port/db_number

> db_number must be in the range of 0 - 15, the default is 0
```go
import (
    "fmt"
    "time"

    . "github.com/go-yaaf/yaaf-common-valkey/valkey"
    . "github.com/go-yaaf/yaaf-common/database"
)

func main() {
    uri := "valkey://localhost:6379/12"
    dc, err := NewValkeyDataCache(uri)
    if err != nil {
        panic(any(err))
    }
	
	dc.Ping(5, 5)
	
}
```