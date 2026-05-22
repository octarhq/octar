package queue

import (
	"fmt"
	"testing"
	"unsafe"
)

// TestMemoryLayout imprime o custo real em bytes de cada estrutura crítica.
// Não é um teste de correctness — é uma régua de memória para decisões de design.
// Execute com: go test ./internal/queue/ -run TestMemoryLayout -v
func TestMemoryLayout(t *testing.T) {
	fmt.Printf("\n=== Tamanho dos structs (unsafe.Sizeof — campos apenas, sem dados de string/slice) ===\n")
	fmt.Printf("Message:       %d bytes\n", unsafe.Sizeof(Message{}))
	fmt.Printf("GroupConfig:   %d bytes\n", unsafe.Sizeof(GroupConfig{}))
	fmt.Printf("group:         %d bytes\n", unsafe.Sizeof(group{}))
	fmt.Printf("GroupStats:    %d bytes\n", unsafe.Sizeof(GroupStats{}))

	fmt.Printf("\n=== Custo real por grupo vazio (inclui alocações internas) ===\n")
	// ready slice com cap=4: 4 * 8 bytes de ponteiros (era cap=64 = 512 bytes)
	readyCap := 4 * 8 // keep in sync with newGroup() in group.go
	// processing map: overhead de map vazio em Go
	processingMapOverhead := 128
	// GroupConfig.Key string data (média "tenant-xxxxxxxx")
	avgKeyData := 16
	// struct fields
	groupStruct := int(unsafe.Sizeof(group{}))
	total := groupStruct + readyCap + processingMapOverhead + avgKeyData
	fmt.Printf("  struct fields:            %d bytes\n", groupStruct)
	fmt.Printf("  ready slice (cap=4):      %d bytes\n", readyCap)
	fmt.Printf("  processing map overhead:  %d bytes\n", processingMapOverhead)
	fmt.Printf("  avg key string data:      %d bytes\n", avgKeyData)
	fmt.Printf("  TOTAL por grupo vazio:    ~%d bytes (~%.1f KB)\n", total, float64(total)/1024)

	fmt.Printf("\n=== Custo por mensagem (payload excluído) ===\n")
	// Struct + strings estáticas (queuename, namespace, groupkey, id)
	msgStruct := int(unsafe.Sizeof(Message{}))
	msgStrings := 16 + 8 + 16 + 8 // ID(hex16) + queue + namespace + groupKey
	fmt.Printf("  struct fields:   %d bytes\n", msgStruct)
	fmt.Printf("  strings data:    ~%d bytes\n", msgStrings)
	fmt.Printf("  TOTAL (sem payload): ~%d bytes\n", msgStruct+msgStrings)

	fmt.Printf("\n=== Projeção de memória por escala ===\n")
	bytesPerGroup := total
	for _, n := range []int{1_000, 10_000, 100_000, 500_000, 1_000_000} {
		mb := float64(n*bytesPerGroup) / (1024 * 1024)
		fmt.Printf("  %7d grupos vazios: %6.1f MB\n", n, mb)
	}

	fmt.Printf("\n=== Rate limiter: custo inicial (cap=min(max,64)) ===\n")
	for _, max := range []int{100, 1_000, 10_000} {
		initCap := min(max, rateLimitInitCap)
		kbNow := float64(initCap*8) / 1024
		kbOld := float64(max*8) / 1024
		fmt.Printf("  rate_limit.max=%-6d → +%.1f KB (era %.1f KB) — cresce sob demanda\n", max, kbNow, kbOld)
	}
	fmt.Printf("\n  Para 10k tenants com max=10000:\n")
	fmt.Printf("    Antes: 10000 × 78.1 KB = %.0f MB alocados imediatamente\n", float64(10000*10000*8)/(1024*1024))
	fmt.Printf("    Agora: 10000 × 0.5 KB  = %.0f MB para tenants idle\n", float64(10000*rateLimitInitCap*8)/(1024*1024))

	fmt.Printf("\n=== Scheduler activation channel ===\n")
	type groupActivationApprox struct {
		q        uintptr   // *queue.Queue
		groupKey [2]uintptr // string
		token    uintptr   // *GroupToken
		enqueued [3]uintptr // time.Time
	}
	chanSlotSize := int(unsafe.Sizeof(groupActivationApprox{}))
	fmt.Printf("  slot size:      ~%d bytes\n", chanSlotSize)
	fmt.Printf("  buffer cap:     65536 slots\n")
	fmt.Printf("  buffer memory:  %.1f MB\n", float64(65536*chanSlotSize)/(1024*1024))
}
