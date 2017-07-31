package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	sampleTime = 2 * time.Second

	traceDir    = "/sys/kernel/debug/tracing/"
	traceFilter = "mark_page_accessed\nmark_buffer_dirty\nadd_to_page_cache_lru\naccount_page_dirtied\n"
)

func writeFile(name string, content string) error {
	file, err := os.OpenFile(name, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	n, err := fmt.Fprint(file, content)
	if err != nil {
		return err
	}
	if len(content) != n {
		return errors.New("mismatch in the number of bytes written")
	}

	return nil
}

func enableTracing() error {
	return writeFile(traceDir+"function_profile_enabled", "1")
}

func disableTracing() error {
	return writeFile(traceDir+"function_profile_enabled", "0")
}

func setFilters() error {
	return writeFile(traceDir+"set_ftrace_filter", traceFilter)
}

type pageCacheStats struct {
	markPageAccessed   float64
	markBufferDirty    float64
	addToPageCacheLRU  float64
	accountPageDirtied float64
}

func readStats(name string, stats *pageCacheStats) error {
	file, err := os.OpenFile(name, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())

		if !strings.Contains(traceFilter, fields[0]) {
			continue
		}

		hits, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return err
		}

		switch fields[0] {
		case "mark_page_accessed":
			stats.markPageAccessed += hits
		case "mark_buffer_dirty":
			stats.markBufferDirty += hits
		case "add_to_page_cache_lru":
			stats.addToPageCacheLRU += hits
		case "account_page_dirtied":
			stats.accountPageDirtied += hits
		}
	}
	return scanner.Err()
}

func reportStats(stats *pageCacheStats) {
	total := stats.markPageAccessed - stats.markBufferDirty
	misses := stats.addToPageCacheLRU - stats.accountPageDirtied
	if misses < 0 {
		misses = 0
	}
	hits := total - misses
	ratio := 100 * hits / total

	fmt.Println(ratio)
}

func analyze_traces() error {
	matches, err := filepath.Glob(traceDir + "trace_stat/function*")
	if err != nil {
		return err
	}

	var stats pageCacheStats
	for _, name := range matches {
		if err := readStats(name, &stats); err != nil {
			return err
		}
	}
	reportStats(&stats)

	return nil
}

func sample() error {
	if err := setFilters(); err != nil {
		return err
	}

	if err := disableTracing(); err != nil {
		return err
	}

	if err := enableTracing(); err != nil {
		return err
	}

	time.Sleep(sampleTime)

	if err := disableTracing(); err != nil {
		return err
	}

	return analyze_traces()
}

func main() {
	defer disableTracing()

	if err := sample(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
