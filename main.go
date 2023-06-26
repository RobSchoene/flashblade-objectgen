package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"runtime"
)

func main() {

	dataVipPtr := flag.String("datavip", "0.0.0.0", "Remote IP address for data connections.")
	bucketPtr := flag.String("bucket", "bucketName", "Remote bucket for S3 testing. Default is to automatically create temporary bucket.")
	numobjectsPtr := flag.Int("objects", 10000, "Number of objects to generate")
	prefixlenPtr := flag.Int("prefix", 32, "Number of random chars for object name prefix")
	flag.Parse()


	// Exit if bucket wasn't specified.
	var bucketName string
	if value, exists := os.LookupEnv("BUCKET_NAME"); exists {
		bucketName = value
	} else if *bucketPtr != "bucketName"{
		bucketName = *bucketPtr
	} else {
		fmt.Println("ERROR. Must set bucket name on command line or in environment variable")
		os.Exit(1)
	}

	// Exit if VIP wasn't specified.
	var dataVips []string
	if value, exists := os.LookupEnv("DATA_VIP"); exists {
		dataVips = []string{value}
	} else if *dataVipPtr != "0.0.0.0" {
		dataVips = []string{*dataVipPtr}
	} else {
		fmt.Println("ERROR. Must provide data VIP on the command line or in environment variable")
		os.Exit(1)
	}

	// Exit if number of objects is <1 or not specified
	var numObjects int
	if value, exists := os.LookupEnv("NUMBER_OBJECTS"); exists {
		numObjectsVar, err := strconv.Atoi(value)
		if err != nil{
			fmt.Println("error", err)
		}
		numObjects = numObjectsVar
	} else if *numobjectsPtr > 0{
		numObjects = *numobjectsPtr
	} else {
		fmt.Println("ERROR. Must set number of objects to be created on command line or in environment variable")
		os.Exit(1)
	}

	// Exit if prefix length is <1 or not specified
	var prefixLength int
	if value, exists := os.LookupEnv("PREFIX_LENGTH"); exists {
		prefixLengthVar, err := strconv.Atoi(value)
		if err != nil{
			fmt.Println("error", err)
		}
		prefixLength = prefixLengthVar
	} else if *prefixlenPtr > 0{
		prefixLength = *prefixlenPtr
	} else {
		fmt.Println("ERROR. Must set the length of the object name prefix on command line or in environment variable")
		os.Exit(1)
	}

	//Get the number of cores for concurrency
	coreCount := runtime.NumCPU()
	if coreCount < 12 {
		fmt.Printf("WARNING. Found %d cores, recommend at least 12 cores to prevent client bottlenecks.\n", coreCount)
	}

	var results []string

	// ===== S3 Tests =====
	accessKey := ""
	secretKey := ""

	for _, dataVip := range dataVips {

		s3, err := NewS3Tester(dataVip, accessKey, secretKey, bucketName, coreCount, numObjects, prefixLength)
		if err != nil {
			fmt.Println(err)

			results = append(results, fmt.Sprintf("%s,s3,FAILED TO CONNECT,-,-", dataVip))
			continue
		}

		fmt.Println("Writing Objects.")
		objectsWritten := s3.WriteTest()
		fmt.Printf("Objects Written = %d\n", objectsWritten)

		results = append(results, fmt.Sprintf("%s,s3,SUCCESS,%d,", dataVip, objectsWritten))
	}

	fmt.Println("\ndataVip,protocol,result,obj_written")
	for _, r := range results {
		fmt.Println(r)
	}
}