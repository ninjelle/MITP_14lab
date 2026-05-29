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
	Title     string    `json:"title"`
	Price     string    `json:"price"`
	Rating    string    `json:"rating"`
	PageURL   string    `json:"page_url"`
	CollectedAt time.Time `json:"collected_at"`
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
					// Рейтинг — в <p class="star-rating One/Two/...">
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
	totalPages := 5

	productsCh := make(chan Product, 200)

	var wg sync.WaitGroup

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
	}()

	os.MkdirAll("../data", 0755)
	file, err := os.Create("../data/products.json")
	if err != nil {
		log.Fatal("Не могу создать файл:", err)
	}
	defer file.Close()

	count := 0
	for product := range productsCh {
		data, err := json.Marshal(product)
		if err != nil {
			log.Printf("Ошибка маршалинга: %v", err)
			continue
		}
		file.Write(data)
		file.WriteString("\n")
		count++
	}

	fmt.Printf("\nГотово! Собрано товаров: %d\n", count)
	fmt.Println("Данные сохранены в data/products.json")
}