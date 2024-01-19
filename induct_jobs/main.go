package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	enqueuedJobsCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "enqueued_jobs_total",
		Help: "Total number of enqueued jobs",
	})
)

func init() {
	// Register Prometheus metrics
	prometheus.MustRegister(enqueuedJobsCounter)
}

type TripData struct {
	ID         int
	SrcCity    string
	DstCity    string
	TripLength int
	StartTime  time.Time
	Duration   string // Change type to string

	// Add any other fields required for processing the job
}

// Enqueue job by inserting into PostgreSQL jobs table
func enqueueJob(db *sql.DB, tripData TripData) (int, error) {
	// Convert job to JSON
	tripDataJSON, err := json.Marshal(tripData)
	if err != nil {
		return 0, err
	}

	// Insert job data into PostgreSQL jobs table with status 'pending' and returning id
	row := db.QueryRow("INSERT INTO jobs (message, status) VALUES ($1, $2) RETURNING id", tripDataJSON, "pending")

	// Retrieve the auto-generated ID
	var id int
	err = row.Scan(&id)
	if err != nil {
		return 0, err
	}

	return id, nil
}

// Get all cities from the database based on the query
func getAllCities(db *sql.DB, query string) ([]string, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cities []string
	for rows.Next() {
		var city string
		err := rows.Scan(&city)
		if err != nil {
			return nil, err
		}
		cities = append(cities, city)
	}

	return cities, nil
}

// Generate all permutations of routes
// Generate all permutations of routes with specified trip length range
func generateAllRoutes(db *sql.DB, minTripLength, maxTripLength int) ([]TripData, error) {
	srcCitiesQuery := "SELECT name FROM public.cities WHERE type = 'source';"
	dstCitiesQuery := "SELECT name FROM public.cities WHERE type = 'destination';"

	srcCities, err := getAllCities(db, srcCitiesQuery)
	if err != nil {
		return nil, err
	}

	dstCities, err := getAllCities(db, dstCitiesQuery)
	if err != nil {
		return nil, err
	}

	var allRoutes []TripData

	for _, srcCity := range srcCities {
		for _, dstCity := range dstCities {
			if srcCity != dstCity {
				for tripLength := minTripLength; tripLength <= maxTripLength; tripLength += 50 {
					// Create TripData without ID initially
					tripData := TripData{
						SrcCity:    srcCity,
						DstCity:    dstCity,
						TripLength: tripLength,
						StartTime:  time.Now(),
					}

					// Enqueue job and get the auto-generated ID
					id, err := enqueueJob(db, tripData)
					if err != nil {
						return nil, err
					}

					// Set the obtained ID in the TripData
					tripData.ID = id

					allRoutes = append(allRoutes, tripData)
				}
			}
		}
	}

	return allRoutes, nil
}

func main() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	// Get the database connection string from environment variables
	dbConnectionString := os.Getenv("DATABASE_URL")

	// PostgreSQL database configuration
	db, err := sql.Open("postgres", dbConnectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Set the range of trip lengths you want to iterate over
	const minTripLength = 6
	const maxTripLength = 15

	// Generate all routes within the specified trip length range
	allRoutes, err := generateAllRoutes(db, minTripLength, maxTripLength)
	if err != nil {
		log.Fatal(err)
	}

	// Print all generated routes
	for _, route := range allRoutes {
		fmt.Printf("Enqueued job: %+v\n", route)

		// Sleep for a short duration between enqueuing jobs if needed
		time.Sleep(time.Second)
	}
}
