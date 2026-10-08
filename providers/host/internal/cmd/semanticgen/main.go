package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kubling-community/kubling-providers/providers/host/internal/semanticmodel"
)

func main() {
	output := flag.String("output", "semantic/host-v1.yaml", "generated semantic fragment path")
	check := flag.Bool("check", false, "verify the generated fragment without writing it")
	flag.Parse()

	document, err := semanticmodel.Generate()
	if err != nil {
		fatal(err)
	}
	if *check {
		current, err := os.ReadFile(*output)
		if err != nil {
			fatal(fmt.Errorf("read %s: %w", *output, err))
		}
		if !bytes.Equal(current, document) {
			fatal(fmt.Errorf("%s is not up to date; run go generate .", *output))
		}
		return
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		fatal(fmt.Errorf("create output directory: %w", err))
	}
	if err := os.WriteFile(*output, document, 0o644); err != nil {
		fatal(fmt.Errorf("write %s: %w", *output, err))
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
