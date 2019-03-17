package main

/**
 Implementation note:    
    cipher/rot128.go (added func DecryptRot128() in cipher package, use to decipher rot128 encoded strings)
    
	The program will take input via command line and reads a file that has encrypted content (e.g. data/fng.csv.rot128)
	It will then decrypt the file, read it in memory and process each donors by calling Omise Token and Charge APIs.

	Entries with invalid/expired credit cards will not be process and will be considered faulty donation
	A summary will be generated at the end to provide brief info on the donation activity.
	
	Usage:
	 $>  go install -v (install deploy package to bin)
	 $>  %GOPATH%/bin/go-tamboon data/fng.1000.csv.rot128
*/

// package imports
import (
    "os"
    "fmt"
	"strings"
	"strconv"
	"sync"
	"sort"
	"io"
	"io/ioutil"
	"bufio"
	"bytes"
	"time"
	"log"
	"runtime/debug"
	cipher "omise/go-tamboon/cipher"
	"github.com/omise/omise-go"
	"github.com/omise/omise-go/operations"
)

// Ideally these can be read via a config file
const (
	OmisePublicKey = "pkey_test_521w1g1t7w4x4rd22z0"
	OmiseSecretKey = "skey_test_521w1g1t6yh7sx4pu8n"
)

// Donor information
type CreateDonor struct {
   Name				string 		`json:"name"`
   AmountSubUnit 	int64		`json:"amount_subunit"`
   Currency			string		`json:"currency"`
   CCNumber			string  	`json:"ccnumber"`
   ExpMonth			time.Month	`json:"expmonth"`
   ExpYear			int			`json:"expyear"`
}

type Promise struct {
   wg sync.WaitGroup
   total int64
   donated int64
}

var instance *Promise
var once sync.Once

var top_donors = struct{
    sync.RWMutex
    m map[int]string
}{m: make(map[int]string)}

/*****************************************
  Make a donation
*****************************************/
func makeDonation(str string, p *Promise) {
    donor_info := strings.Split(str, ",")
	
	client, err := omise.NewClient(OmisePublicKey, OmiseSecretKey)
	if err != nil {
		log.Fatal(err)
	}
	
	amt, _ := strconv.ParseInt(donor_info[1], 10, 64)
	cc_expmonth, _ := strconv.Atoi(donor_info[4])
	cc_expyear, _ := strconv.Atoi(donor_info[5])
	
	donor := newDonor(donor_info[0], amt, "thb", donor_info[2], time.Month(cc_expmonth), cc_expyear)

	token, createToken := &omise.Token{}, &operations.CreateToken{
		Name:            donor.Name,
		Number:          donor.CCNumber,
		ExpirationMonth: donor.ExpMonth,
		ExpirationYear:  donor.ExpYear,
	}

	// handle invalid/expired credit cards
	if err := client.Do(token, createToken); err != nil {
		// log.Println(err)
	}
	
	// Creates a charge from the token
	charge, createCharge := &omise.Charge{}, &operations.CreateCharge {
		Amount:  donor.AmountSubUnit,
		Currency: donor.Currency,
		Card:     token.ID,
	}
	
	if err := client.Do(charge, createCharge); err != nil {
		// fmt.Println(err)
	}
	
    // enable this line to output donor info on the console
	// log.Printf("Donors: %s  charge: %s  amount: %s %d\n", donor.Name, charge.ID, charge.Currency, charge.Amount)
	
	top_donors.Lock()
	top_donors.m[int(charge.Amount)] = donor.Name
	top_donors.Unlock()
	
	p.total += donor.AmountSubUnit
	p.donated += charge.Amount
	
	donor = CreateDonor{}
	createToken = nil
	createCharge = nil
}

/**********************************************
 Creates a new donor
**********************************************/
func newDonor(name string, amt int64, curr string, cc string, expmonth time.Month, expyear int) CreateDonor {
	return CreateDonor{name, amt, curr, cc, expmonth, expyear}
}

/**********************************************
 Pick top 3 donors
**********************************************/
func pickTopDonors(m map[int]string, count int, f func(k int, v string)) {
    var keys []int
    for k, _ := range m {
        keys = append(keys, k)
    }
    sort.Ints(keys)
	for _, k := range keys {
		if count < (len(m) - 3) {
		   count++
		   continue
		}
		f(k, m[k])
    }
}

/********************************************
Get a singleton instance a Promise object
********************************************/
func getInstance() *Promise {
  once.Do(func() {
     instance = &Promise{}
  })
  return instance
}

/*********************************************
 Returns a promise object
*********************************************/
func GetPromise(str string) *Promise {
   p := getInstance()
   p.wg.Add(1)
   go func() {
      makeDonation(str, p)
	  defer p.wg.Done()
   }()
   return p
}

/*********************************************
 Process callback when a promise returns
*********************************************/
func (p *Promise) Then (r func(int64, int64)) {
   p.wg.Wait()
   r(p.total, p.donated)
}

/*********************************************
 Count number of lines from a decrypted data
*********************************************/
func lineCounter(r io.Reader) (int, error) {
    buf := make([]byte, 32*1024)
    count := 0
    lineSep := []byte{'\n'}
    for {
        c, err := r.Read(buf)
        count += bytes.Count(buf[:c], lineSep)

        switch {
        case err == io.EOF:
            return count, nil

        case err != nil:
            return count, err
        }
    }
}

/******************************************
 Main func
******************************************/
func main() {

    csv_file := os.Args[1]

	log.Printf("performing donations...\n")
	
    encrypted_data, err := ioutil.ReadFile(csv_file)
    
    if err != nil {
        fmt.Println("Error reading the file!", err)
        return
    }
	
	reader := strings.NewReader(string(cipher.DecryptRot128(encrypted_data)))
	
	numlines, err := lineCounter(reader)
	if err != nil {
	   log.Println("error :", err)
	}
	
	reader.Seek(0, io.SeekStart)
	
    scanner := bufio.NewScanner(reader)
	scanner.Scan()
	
	var p *Promise
	r_limiter := time.Tick(time.Millisecond * 150)
	
	for scanner.Scan() {
		<-r_limiter
		p = GetPromise(scanner.Text())
	}
	
	p.Then(
	  func(total int64, donated int64) {
	     // calculate faulty donations, avg amount donated per donor
	     faulty_donations := total - donated
	     avg := donated / int64(numlines-1)
	
	     // Show donation summary report:
		 log.Printf("\n")
	     log.Printf("      total received: THB %d\n", total)
	     log.Printf("successfully donated: THB %d\n", donated)
	     log.Printf("     faulty donation: THB %d\n", faulty_donations)
	     log.Printf("  average per person: THB %d\n", avg)
         log.Printf("          top donors: ")
	
	     count := 0;
	     pickTopDonors(top_donors.m, count,
            func(k int, v string) { log.Printf("\t\t%s\n", v) })
		
        log.Printf("done!")
	  })
	
	// clean-up
	p = nil
    debug.FreeOSMemory()
}