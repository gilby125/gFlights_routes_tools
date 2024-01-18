package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gilby125/google-flights-api/flights"
	"github.com/joho/godotenv"
	"github.com/lib/pq"
	"golang.org/x/text/currency"
	"golang.org/x/text/language"
)

const MaxQueueSize = 100

type TripData struct {
	ID         int
	SrcCity    string
	DstCity    string
	TripLength int
	StartTime  time.Time
	Duration   string // Change type to string
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	connectionString := os.Getenv("DATABASE_URL")

	lang := language.English

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a job queue with a mutex for synchronization
	var mu sync.Mutex
	jobQueue := make(chan TripData, MaxQueueSize)

	// Create a wait group
	var wg sync.WaitGroup

	// Create a context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	handleGracefulShutdown(cancel)

	// Start PostgreSQL job processing in a separate goroutine
	go processJobsFromPostgresNotification(ctx, jobQueue, db, lang, &mu, &wg)

	// Create worker goroutines
	numWorkers := 5
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(jobQueue, &wg, lang, db, ctx, &mu)
	}

	// Start HTTP server in a separate goroutine
	go startHTTPServer()

	// Wait for all workers to finish or cancellation
	wg.Wait()
	close(jobQueue)

	log.Println("All workers have finished. Exiting.")
}

func handleGracefulShutdown(cancel context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Received interrupt signal. Initiating shutdown.")
		cancel() // Cancel the context to signal workers to exit
	}()
}

func startHTTPServer() {
	http.HandleFunc("/progress", progressHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting HTTP server on port %s", port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatalf("Error starting HTTP server: %v", err)
	}
}

func progressHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	message := map[string]string{"progress": "Processing in progress"}
	json.NewEncoder(w).Encode(message)
}

func processJobsFromPostgresNotification(ctx context.Context, jobQueue chan TripData, db *sql.DB, lang language.Tag, mu *sync.Mutex, wg *sync.WaitGroup) {
	for {
		err := setupPostgresListener(ctx, jobQueue, db, lang, mu, wg)
		if err != nil {
			log.Printf("Error setting up PostgreSQL listener: %v", err)
			log.Printf("Attempting to reconnect in 5 minutes...")
			time.Sleep(5 * time.Minute) // Wait before attempting to reconnect
		}
	}
}

// ...

func setupPostgresListener(ctx context.Context, jobQueue chan TripData, db *sql.DB, lang language.Tag, mu *sync.Mutex, wg *sync.WaitGroup) error {
	err := godotenv.Load()
	if err != nil {
		return fmt.Errorf("Error loading .env file: %v", err)
	}

	connectionString := os.Getenv("DATABASE_URL")

	listener := pq.NewListener(connectionString, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("PostgreSQL listener error: %v\n", err)
		}
	})

	err = listener.Listen("jobs_channel")
	if err != nil {
		return fmt.Errorf("Error listening to PostgreSQL channel: %v", err)
	}

	defer listener.Close()

	log.Printf("Connected to PostgreSQL")

	for {
		select {
		case <-ctx.Done():
			log.Println("PostgreSQL job processing stopped.")
			return nil
		case notify := <-listener.Notify:
			// Handle the notification and retrieve the job data
			var job TripData
			err := json.Unmarshal([]byte(notify.Extra), &job)
			if err != nil {
				log.Printf("Error unmarshalling job JSON: %v", err)
				continue
			}

			log.Printf("Received job from PostgreSQL: %+v")

			// Pass the context, lang, and mutex to the worker function
			workerContext, cancel := context.WithCancel(ctx)
			defer cancel()

			// Send job to the internal queue with mutex lock
			mu.Lock()
			select {
			case jobQueue <- job:
				log.Println("Job sent to internal queue")
			default:
				log.Println("Job queue is full. Skipping job.")
			}
			mu.Unlock()

			// Start a goroutine to process the job
			wg.Add(1)
			go func() {
				defer cancel()
				defer wg.Done()
				worker(jobQueue, &sync.WaitGroup{}, lang, db, workerContext, mu)
			}()
		}
	}
}

func processJobs(ctx context.Context, jobQueue chan TripData, db *sql.DB, lang language.Tag, mu *sync.Mutex, wg *sync.WaitGroup) error {
	err := godotenv.Load()
	if err != nil {
		return fmt.Errorf("Error loading .env file: %v", err)
	}

	connectionString := os.Getenv("DATABASE_URL")

	listener := pq.NewListener(connectionString, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("PostgreSQL listener error: %v\n", err)
		}
	})

	err = listener.Listen("jobs_channel")
	if err != nil {
		return fmt.Errorf("Error listening to PostgreSQL channel: %v", err)
	}

	defer listener.Close()

	log.Printf("Connected to PostgreSQL")

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Println("PostgreSQL job processing stopped.")
				return
			case notify := <-listener.Notify:
				// Handle the notification and retrieve the job data
				var job TripData
				err := json.Unmarshal([]byte(notify.Extra), &job)
				if err != nil {
					log.Printf("Error unmarshalling job JSON: %v", err)
					continue
				}

				log.Printf("Received job from PostgreSQL: %+v")

				// Pass the context, lang, and mutex to the worker function
				workerContext, cancel := context.WithCancel(ctx)
				defer cancel()

				// Send job to the internal queue with mutex lock
				mu.Lock()
				select {
				case jobQueue <- job:
					log.Println("Job sent to internal queue")
				default:
					log.Println("Job queue is full. Skipping job.")
				}
				mu.Unlock()

				// Start a goroutine to process the job
				wg.Add(1)
				go func() {
					defer cancel()
					defer wg.Done()
					worker(jobQueue, &sync.WaitGroup{}, lang, db, workerContext, mu)
				}()
			}
		}
	}()

	<-ctx.Done()
	return nil
}

func insertOfferData(db *sql.DB, srcCity, dstCity string, startDate, returnDate time.Time, price int, url, className string) error {
	_, err := db.Exec(`
        INSERT INTO offers (srccty, dstcty, start_date, return_date, price, url, timestamp, class)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (srccty, dstcty, start_date, return_date, price, class) DO UPDATE
        SET price = CASE WHEN EXCLUDED.price < offers.price THEN EXCLUDED.price ELSE offers.price END,
            timestamp = CASE WHEN EXCLUDED.price < offers.price THEN NOW() ELSE offers.timestamp END
    `,
		srcCity, dstCity, startDate, returnDate, price, url, time.Now(), className,
	)
	return err
}

func worker(jobQueue chan TripData, wg *sync.WaitGroup, lang language.Tag, db *sql.DB, ctx context.Context, mu *sync.Mutex) {
	defer wg.Done()

	log.Println("Worker started")

	for {
		select {
		case <-ctx.Done():
			log.Println("Worker received cancellation signal. Exiting.")
			return
		case job, ok := <-jobQueue:
			if !ok {
				log.Println("Job queue closed. Exiting.")
				return
			}

			// Lock the mutex before accessing the job queue
			mu.Lock()
			log.Printf("Worker received job: %+v", job)

			// Call the getCheapestOfferAndInsert function with the worker's context
			workerContext, cancel := context.WithCancel(ctx)
			defer cancel()
			getCheapestOfferAndInsert(
				workerContext,
				time.Now().AddDate(0, 0, 60),
				time.Now().AddDate(0, 0, 180),
				[]int{job.TripLength},
				job.SrcCity,
				job.DstCity,
				lang,
				db,
			)

			// Update job status to 'processed' in the database
			if err := markJobAsProcessed(db, job); err != nil {
				log.Printf("Error marking job as processed: %v", err)
			}

			// Remove processed jobs from the database
			if err := removeProcessedJobs(db); err != nil {
				log.Printf("Error removing processed jobs: %v", err)
			}

			log.Println("Job processed successfully")

			// Unlock the mutex after processing the job
			mu.Unlock()
		}
	}
}

func printOfferDetails(srcCity, dstCity string, tripLength int, className string, bestOffer flights.Offer) {
	fmt.Printf("From: %s, To: %s\n", srcCity, dstCity)
	fmt.Printf("Trip Length: %d days\n", tripLength)
	fmt.Printf("%s %s\n", bestOffer.StartDate, bestOffer.ReturnDate)
	fmt.Printf("Price: %d\n", int(bestOffer.Price))
}
func getCheapestOfferAndInsert(ctx context.Context, rangeStartDate, rangeEndDate time.Time, tripLengths []int, srcCity, dstCity string, lang language.Tag, db *sql.DB) {
	session, err := flights.New()
	if err != nil {
		log.Printf("Error creating flights session: %v", err)
		return
	}

	for _, tripLength := range tripLengths {
		for _, class := range []flights.Class{flights.Business, flights.Economy} {
			select {
			case <-ctx.Done():
				log.Println("Cancellation signal received. Exiting getCheapestOfferAndInsert.")
				return
			default:
				// Continue processing
			}

			options := flights.Options{
				Travelers: flights.Travelers{Adults: 1},
				Currency:  currency.USD,
				Stops:     flights.Stop2,
				Class:     class,
				TripType:  flights.RoundTrip,
				Lang:      lang,
			}

			offers, err := session.GetPriceGraph(
				ctx,
				flights.PriceGraphArgs{
					RangeStartDate: rangeStartDate,
					RangeEndDate:   rangeEndDate,
					TripLength:     tripLength,
					SrcCities:      []string{srcCity},
					DstCities:      []string{dstCity},
					Options:        options,
				},
			)

			if err != nil {
				log.Printf("Error getting price graph: %v", err)
				continue
			}

			var bestOffer flights.Offer
			for _, o := range offers {
				if o.Price != 0 && (bestOffer.Price == 0 || o.Price < bestOffer.Price) {
					bestOffer = o
				}
			}

			printOfferDetails(srcCity, dstCity, tripLength, fmt.Sprintf("%s", class), bestOffer)

			url, err := session.SerializeURL(
				ctx,
				flights.Args{
					Date:       bestOffer.StartDate,
					ReturnDate: bestOffer.ReturnDate,
					SrcCities:  []string{srcCity},
					DstCities:  []string{dstCity},
					Options:    options,
				},
			)
			if err != nil {
				log.Printf("Error serializing URL: %v", err)
				continue
			}

			fmt.Println(url)
			fmt.Println("------------------------")

			err = insertOfferData(db, srcCity, dstCity, bestOffer.StartDate, bestOffer.ReturnDate, int(bestOffer.Price), url, fmt.Sprintf("%s", class))
			if err != nil {
				log.Printf("Error inserting data: %v", err)
				continue
			}
		}
	}
}

func markJobAsProcessed(db *sql.DB, job TripData) error {
	query := "UPDATE jobs SET status = $1 WHERE id = $2"
	log.Printf("Executing SQL query: %s with parameters: status=%s, id=%d", query, "processed", job.ID)

	_, err := db.Exec(query, "processed", job.ID)
	if err != nil {
		log.Printf("Error updating job status to 'processed': %v", err)
		return err
	}

	log.Printf("Job with ID %d marked as processed", job.ID)
	return nil
}

func removeProcessedJobs(db *sql.DB) error {
	result, err := db.Exec("DELETE FROM jobs WHERE status = $1", "processed")
	if err != nil {
		log.Printf("Error deleting processed jobs: %v", err)
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Printf("Error getting RowsAffected: %v", err)
		return err
	}

	log.Printf("Deleted %d processed jobs from the jobs table", rowsAffected)
	return nil
}
