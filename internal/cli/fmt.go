package cli

import (
	"encoding/json"
	"fmt"
	"os"
)

type OutputFormat int

const (
	FormatText OutputFormat = iota
	FormatJSON
)

var GlobalFormat OutputFormat = FormatText

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func printTable(headers []string, rows [][]string) {
	if GlobalFormat == FormatJSON {
		var objs []map[string]any
		for _, row := range rows {
			obj := make(map[string]any)
			for i, h := range headers {
				obj[h] = row[i]
			}
			objs = append(objs, obj)
		}
		printJSON(objs)
		return
	}

	cols := len(headers)
	widths := make([]int, cols)
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	printRow(headers, widths, " ")
	fmt.Println(stringsRepeat("─", sumWidths(widths, cols)+cols-1))
	for _, row := range rows {
		printRow(row, widths, " ")
	}
}

func printRow(cells []string, widths []int, sep string) {
	for i, cell := range cells {
		if i > 0 {
			fmt.Print(sep)
		}
		fmt.Print(cell)
		if len(cell) < widths[i] {
			fmt.Print(stringsRepeat(" ", widths[i]-len(cell)))
		}
	}
	fmt.Println()
}

func stringsRepeat(s string, n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n*len(s))
	for i := 0; i < n; i++ {
		copy(b[i*len(s):], s)
	}
	return string(b)
}

func sumWidths(widths []int, n int) int {
	s := 0
	for i := 0; i < n; i++ {
		s += widths[i]
	}
	return s
}
