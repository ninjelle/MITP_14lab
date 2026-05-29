package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

type Product struct {
	Title       string    `json:"title"`
	Price       string    `json:"price"`
	Rating      string    `json:"rating"`
	PageURL     string    `json:"page_url"`
	CollectedAt time.Time `json:"collected_at"`
}

type BatchWriter struct {
	file        *os.File
	buffer      []Product
	bufferSize  int         
	flushInterval time.Duration 
	ticker      *time.Ticker
	mu          sync.Mutex
	stopCh      chan struct{}
}

func NewBatchWriter(filename string, bufferSize int, flushInterval time.Duration) (*BatchWriter, error) {
	
	os.MkdirAll("../data", 0755)
	
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	bw := &BatchWriter{
		file:          file,
		buffer:        make([]Product, 0, bufferSize),
		bufferSize:    bufferSize,
		flushInterval: flushInterval,
		ticker:        time.NewTicker(flushInterval),
		stopCh:        make(chan struct{}),
	}

	go bw.autoFlush()

	return bw, nil
}

func (bw *BatchWriter) Add(product Product) {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	bw.buffer = append(bw.buffer, product)

	if len(bw.buffer) >= bw.bufferSize {
		bw.flush()
	}
}

func (bw *BatchWriter) flush() {
	if len(bw.buffer) == 0 {
		return
	}

	for _, product := range bw.buffer {
		data, err := json.Marshal(product)
		if err != nil {
			log.Printf("Ошибка маршалинга: %v", err)
			continue
		}
		bw.file.Write(data)
		bw.file.WriteString("\n")
	}

	fmt.Printf("📦 Сброшено в файл: %d записей\n", len(bw.buffer))
	bw.buffer = bw.buffer[:0] // очищаем буфер
}

func (bw *BatchWriter) autoFlush() {
	for {
		select {
		case <-bw.ticker.C:
			bw.mu.Lock()
			bw.flush()
			bw.mu.Unlock()
		case <-bw.stopCh:
			return
		}
	}
}

func (bw *BatchWriter) Close() error {
	bw.ticker.Stop()
	close(bw.stopCh)

	bw.mu.Lock()
	defer bw.mu.Unlock()
	bw.flush()

	return bw.file.Close()
}

func parsePage(pageNum int) ([]Product, error) {
	url := fmt.Sprintf("http://books.toscrape.com/catalogue/page-%d.html", pageNum)
	if pageNum == 1 {
		url = "http://books.toscrape.com/catalogue/page-1.html"
	}

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("статус %d для страницы %d", resp.StatusCode, pageNum)
	}

	doc, err := html.Parse(resp.Body)
	if err != nil {
		return nil, err
	}

	var products []Product

	var traverse func(*html.Node)
	traverse = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "article" {
			for _, attr := range n.Attr {
				if attr.Key == "class" && strings.Contains(attr.Val, "product_pod") {
					p := extractProduct(n, url)
					products = append(products, p)
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			traverse(c)
		}
	}
	traverse(doc)

	return products, nil
}

func extractProduct(n *html.Node, pageURL string) Product {
	p := Product{
		PageURL:     pageURL,
		CollectedAt: time.Now(),
	}

	var traverse func(*html.Node)
	traverse = func(node *html.Node) {
		if node.Type == html.ElementNode {
			switch node.Data {
			case "h3":
				if a := node.FirstChild; a != nil && a.Data == "a" {
					for _, attr := range a.Attr {
						if attr.Key == "title" {
							p.Title = attr.Val
						}
					}
				}
			case "p":
				for _, attr := range node.Attr {
					if attr.Key == "class" && attr.Val == "price_color" {
						if node.FirstChild != nil {
							p.Price = node.FirstChild.Data
						}
					}
					if attr.Key == "class" && strings.Contains(attr.Val, "star-rating") {
						parts := strings.Split(attr.Val, " ")
						if len(parts) == 2 {
							p.Rating = parts[1]
						}
					}
				}
			}
		}
		for c := node.FirstChild; c != nil; c = c.NextSibling {
			traverse(c)
		}
	}
	traverse(n)

	return p
}

func main() {
	totalPages := 20
	
	batchWriter, err := NewBatchWriter("../data/products.json", 10, 3*time.Second)
	if err != nil {
		log.Fatal("Не могу создать BatchWriter:", err)
	}
	defer batchWriter.Close()

	productsCh := make(chan Product, 200)
	var wg sync.WaitGroup

	fmt.Println("🚀 Запуск параллельного сбора данных...")
	fmt.Printf("   Пакетная запись: %d записей или каждые %v\n\n", 10, 3*time.Second)

	startTime := time.Now()

	for i := 1; i <= totalPages; i++ {
		wg.Add(1)
		go func(page int) {
			defer wg.Done()

			fmt.Printf("Парсим страницу %d...\n", page)
			products, err := parsePage(page)
			if err != nil {
				log.Printf("Ошибка на странице %d: %v", page, err)
				return
			}

			for _, p := range products {
				productsCh <- p
			}
			fmt.Printf("Страница %d готова, товаров: %d\n", page, len(products))
		}(i)
	}

	go func() {
		wg.Wait()
		close(productsCh)
		fmt.Println("\nВсе страницы обработаны, завершаем сбор...")
	}()

	count := 0
	for product := range productsCh {
		batchWriter.Add(product)
		count++
		
		if count%50 == 0 {
			fmt.Printf("📊 Прогресс: собрано %d товаров\n", count)
		}
	}
	elapsed := time.Since(startTime)
	fmt.Printf("\nГотово!\n")
	fmt.Printf("   Собрано товаров: %d\n", count)
	fmt.Printf("   Затрачено времени: %v\n", elapsed)
	fmt.Printf("   Данные сохранены в data/products.json (пакетная запись)\n")
}