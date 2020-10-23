package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"strings"
	"gopkg.in/yaml.v2"
	"log"
	"bufio"
	"path"
	"time"
    "context"
    "encoding/json"
    "reflect"
    "strconv"
	"math/rand"

    // Import the Elasticsearch library packages
    "github.com/elastic/go-elasticsearch"
    "github.com/elastic/go-elasticsearch/esapi"
)

var logGenCofigYmlDemo = `
outputDir: "./logs"
modes:
  - python
  - mango
`

// LogGenConfig ...
type LogGenConfig struct {
	OutputDir string `yaml:"outputDir"`
	Modes []string `yaml:"modes"`
}

func pythonLogGen(logDir string) {
	msg := "this is a python log\n"
	if f, err := os.Create(path.Join(logDir, "python.log")); err != nil {
		log.Fatal("Error when create file", err)
	} else {
		w := bufio.NewWriter(f)
		for {
			if _, err := w.WriteString(msg); err != nil {
				log.Fatal("Error while writing to file", err)
			} else {
				w.Flush()
			}
			// sleep for 2 s
			time.Sleep(time.Duration(2)*time.Second)
		}
	}
}

func mangoLogGen(logDir string) {
	// TODO:
}

func main() {
	startLogGen()
}

func startLogGen() {
	config := LogGenConfig{}
	if err := yaml.Unmarshal([]byte(logGenCofigYmlDemo), &config); err != nil {
		log.Fatal("Error when parse yml: ", err)
	}
	fmt.Printf("--- t:\n%+v\n\n", config)
	if err := os.MkdirAll(config.OutputDir, os.ModePerm); err != nil {
		log.Fatal("Error while create directories: ", err)
	}
	for _, mode := range config.Modes {
		switch mode {
		case "python":
			go pythonLogGen(config.OutputDir)
		case "mango":
			go mangoLogGen(config.OutputDir)
		default:
			log.Fatal("Invalid Mode:", mode)
		}
	}
	docCh := make(chan ElasticDocs)
	go sendToEs(docCh)
	startComsumer(docCh)
}

func startComsumer(docCh chan ElasticDocs) {
	config := sarama.NewConfig()
	config.ClientID = "go-kafka-consumer"
	config.Consumer.Return.Errors = true

	brokers := []string{"localhost:9092"}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	topics, _ := master.Topics()

	consumer, errors := consume(topics, master)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case msg := <-consumer:
				msgCount++
				fmt.Printf("Received messages %+v\n", msg)
				fmt.Println("Msg.Value", string(msg.Value))
				docCh<-ElasticDocs{
					Msg: string(msg.Value),
					Timestamp: "xxxx-xx-xx",
				}
			case consumerError := <-errors:
				msgCount++
				fmt.Println("Received consumerError ", string(consumerError.Topic), string(consumerError.Partition), consumerError.Err)
				doneCh <- struct{}{}
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}

func consume(topics []string, master sarama.Consumer) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError)
	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}
		partitions, _ := master.Partitions(topic)
		// this only consumes partition no 1, you would probably want to consume all partitions
		consumer, err := master.ConsumePartition(topic, partitions[0], sarama.OffsetOldest)
		if nil != err {
			fmt.Printf("Topic %v Partitions: %v", topic, partitions)
			panic(err)
		}
		fmt.Println(" Start consuming topic ", topic)
		go func(topic string, consumer sarama.PartitionConsumer) {
			for {
				select {
				case consumerError := <-consumer.Errors():
					errors <- consumerError
					fmt.Println("consumerError: ", consumerError.Err)

				case msg := <-consumer.Messages():
					consumers <- msg
					fmt.Println("Got message on topic ", topic, msg.Value)
				}
			}
		}(topic, consumer)
	}

	return consumers, errors
}

// Declare a struct for Elasticsearch fields
type ElasticDocs struct {
    Msg  string
    Timestamp string
}

// A function for marshaling structs to JSON string
func jsonStruct(doc ElasticDocs) string {

	// Create struct instance of the Elasticsearch fields struct object
	// FIXME: why not create direclty from input doc?
    docStruct := &ElasticDocs{
        Msg: doc.Msg,
        Timestamp: doc.Timestamp,
    }

    fmt.Println("\ndocStruct:", docStruct)
    fmt.Println("docStruct TYPE:", reflect.TypeOf(docStruct))

    // Marshal the struct to JSON and check for errors
    b, err := json.Marshal(docStruct)
    if err != nil {
        fmt.Println("json.Marshal ERROR:", err)
        return string(err.Error())
    }
    return string(b)
}

func sendToEs(docCh chan ElasticDocs) {

    // Allow for custom formatting of log output
    log.SetFlags(0)

    // Create a context object for the API calls
    ctx := context.Background()

    // Create a mapping for the Elasticsearch documents
    var (
        docMap map[string]interface{}
    )
    fmt.Println("docMap:", docMap)
    fmt.Println("docMap TYPE:", reflect.TypeOf(docMap))

    // Declare an Elasticsearch configuration
    cfg := elasticsearch.Config{
        Addresses: []string{
            "http://localhost:9200",
        },
    }

    // Instantiate a new Elasticsearch client object instance
    client, err := elasticsearch.NewClient(cfg)

    if err != nil {
        fmt.Println("Elasticsearch connection error:", err)
    }

    // Have the client instance return a response
    if res, err := client.Info(); err != nil {
        log.Fatalf("client.Info() ERROR:", err)
    } else {
        log.Printf("client response:", res)
    }

    // // Declare documents to be indexed using struct
    // doc1 := ElasticDocs{}
    // doc1.Msg = "A msg"
    // doc1.Timestamp = "xxxx-xx-xx"

    // doc2 := ElasticDocs{}
    // doc2.Msg = "A msg"
    // doc2.Timestamp = "xxxx-xx-xx"
	for {
		doc := <-docCh 

		// Marshal Elasticsearch document struct objects to JSON string
		body := jsonStruct(doc) 

		// FIXME: change documentId as automatically genrated
		docId := rand.Int()

		// Instantiate a request object
		req := esapi.IndexRequest{
			Index:      "some_index",
			DocumentID: strconv.Itoa(docId),
			Body:       strings.NewReader(body),
			Refresh:    "true",
		}
		fmt.Println(reflect.TypeOf(req))

		// Return an API response object from request
		res, err := req.Do(ctx, client)
		if err != nil {
			log.Fatalf("IndexRequest ERROR: %s", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			log.Printf("%s ERROR indexing document ID=%d", res.Status(), docId)
		} else {
			// Deserialize the response into a map.
			var resMap map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&resMap); err != nil {
				log.Printf("Error parsing the response body: %s", err)
			} else {
				log.Printf("\nIndexRequest() RESPONSE:")
				// Print the response status and indexed document version.
				fmt.Println("Status:", res.Status())
				fmt.Println("Result:", resMap["result"])
				fmt.Println("Version:", int(resMap["_version"].(float64)))
				fmt.Println("resMap:", resMap)
				fmt.Println("\n")
			}
		}
	}
}

