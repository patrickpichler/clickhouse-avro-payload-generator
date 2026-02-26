package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
)

type Person struct {
	Name   string `avro:"name"`
	Height uint64 `avro:"height"`
}

func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}

func main() {
	p := Person{
		Name:   "Philip",
		Height: 175,
	}

	schema, err := avro.NewRecordSchema("person", "", []*avro.Field{
		Must(avro.NewField("name", avro.NewPrimitiveSchema(avro.String, nil))),
		Must(avro.NewField("height", Must(avro.NewFixedSchema("height", "", 8, nil)))),
	})

	fmt.Println(schema.String())

	if err != nil {
		panic(err)
	}

	b := bytes.NewBuffer(nil)

	enc, err := ocf.NewEncoderWithSchema(schema, b, ocf.WithCodec(ocf.Snappy))

	if err := enc.Encode(p); err != nil {
		panic(err)
	}
	if err := enc.Encode(p); err != nil {
		panic(err)
	}
	if err := enc.Encode(p); err != nil {
		panic(err)
	}

	enc.Flush()

	os.WriteFile("person.avro", b.Bytes(), 0666)
}
