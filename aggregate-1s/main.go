package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

// Agg -
type Agg struct {
	V   int64     `json:"v"`  // Tick Volume
	VW  float64   `json:"vw"` // VWAP (Volume Weighted Average Price)
	O   float64   `json:"o"`  // Tick Open Price
	C   float64   `json:"c"`  // Tick Close Price
	H   float64   `json:"h"`  // Tick High Price
	L   float64   `json:"l"`  // Tick Low Price
	X   []int     `json:"x"`  // exchanges
	N   int64     `json:"n"`  // Number of Ticks
	T   int64     `json:"t"`  // Timestamp ( Unix MS )
	TAt time.Time // Timestamp ( Unix MS )
}

// AggData -
type AggData struct {
	Aggregates []struct {
		V   int64     `json:"v"`  // Tick Volume
		VW  float64   `json:"vw"` // VWAP (Volume Weighted Average Price)
		O   float64   `json:"o"`  // Tick Open Price
		C   float64   `json:"c"`  // Tick Close Price
		H   float64   `json:"h"`  // Tick High Price
		L   float64   `json:"l"`  // Tick Low Price
		X   []int     `json:"x"`  // exchanges
		N   int64     `json:"n"`  // Number of Ticks
		T   int64     `json:"t"`  // Timestamp ( Unix MS )
		TAt time.Time // Timestamp ( Unix MS )
	} `json:"results"`
}

// Trades - https://polygon.io/docs/stocks/get_v3_trades__stockticker
type Trades struct {
	Results []struct {
		T int64   `json:"t"` // The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this message from the exchange which produced it.
		Y int64   `json:"y"` // The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the quote was actually generated at the exchange.
		Q int     `json:"q"` // The sequence number representing the sequence in which trade events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11).
		I string  `json:"i"` // The Trade ID which uniquely identifies a trade. These are unique per combination of ticker, exchange, and TRF. For example: A trade for AAPL executed on NYSE and a trade for AAPL executed on NASDAQ could potentially have the same Trade ID.
		X int     `json:"x"` // The exchange ID. See Exchanges for Polygon.io's mapping of exchange IDs.
		S int     `json:"s"` // The size of a trade (also known as volume) as a number of whole shares traded.
		C []int   `json:"c"` // conditions
		P float64 `json:"p"` // The price of the trade. This is the actual dollar value per whole share of this trade. A trade of 100 shares with a price of $2.00 would be worth a total dollar value of $200.00.
		Z int     `json:"z"` // There are 3 tapes which define which exchange the ticker is listed on. These are integers in our objects which represent the letter of the alphabet. Eg: 1 = A, 2 = B, 3 = C.
	} `json:"results"`
}

func main() {

	var data Trades
	var agg AggData

	jsonFile, err := os.Open("AMC-2022-12-22.json")

	if err != nil {
		panic(err)
	}

	defer jsonFile.Close()

	// read in data dump
	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &data)

	// logic
	// read in raw tick data
	// loop through each tick tracking hour,min,sec
	// if event in this second
	//
	// ticks volume
	// open
	// close
	// high
	// low
	// timestamp unix
	// time.Time

	total := len(data.Results) // total results
	var hour int               // record hour
	var minute int             // record min
	var second int             // record sec
	var size int64             // volume
	var open float64           // first record
	var close float64          // last
	var high float64           // sec high
	var low float64            // sec low
	var count int64
	var exchanges []int // exchanges seen

	for i := 0; i < total; i++ {

		// parse time
		t := data.Results[i].T / 1000000
		tickTime := time.Unix(0, t*int64(time.Millisecond)) // time.Unix(0, msg.T)
		//tickTime := time.Unix(0, int64(data.Results[i].T)) // time.Unix(0, msg.T)

		// first tick - set our initial values
		if i == 0 {

			// timing
			hour = tickTime.Hour()
			minute = tickTime.Minute()
			second = tickTime.Second()

			// metadata
			open = data.Results[i].P
			close = data.Results[i].P
			high = data.Results[i].P
			low = data.Results[i].P
			size = int64(data.Results[i].S) // init volume
			exchanges = appendStringIfMissing(exchanges, data.Results[i].X)

			count++

			// next loop
			continue
		}

		// are we still in the same time window (as the previose loop)?
		if tickTime.Hour() == hour && tickTime.Minute() == minute && tickTime.Second() == second {
			//if tickTime.Hour() == hour && tickTime.Minute() == minute {

			// we set this at every loop, but the last one will remain
			close = data.Results[i].P

			// high
			if high < data.Results[i].P && data.Results[i].P != 0 {
				high = data.Results[i].P
			}

			// low
			if low > data.Results[i].P && data.Results[i].P != 0 {
				low = data.Results[i].P
			}

			// add exchange
			exchanges = appendStringIfMissing(exchanges, data.Results[i].X)

			// same time window
			size = size + int64(data.Results[i].S) // add to vomume for window

			count++

			// next loop
			continue

		} else {

			// new time window; save prev record
			var agm Agg
			agm.V = size // Tick Size Volume
			agm.O = open // Tick Open Price
			//agm.C = data.Results[i-1].P // Tick Close Price (set to last tick price)
			agm.C = close // Tick Close Price (set to last tick price)
			agm.H = high  // Tick High Price
			agm.L = low   // Tick Low Price
			agm.N = count
			agm.X = exchanges // set exchanges

			// get last tick timestamp
			ct := data.Results[i-1].T / 1000000
			ctTime := time.Unix(0, ct*int64(time.Millisecond)) // time.Unix(0, msg.T)
			agm.T = ct                                         // Timestamp ( Unix MS ) // remove 6 ditigs from the right
			agm.TAt = ctTime

			// append record
			agg.Aggregates = append(agg.Aggregates, agm)
			//fmt.Println(agm)

			// reset timing
			hour = tickTime.Hour()
			minute = tickTime.Minute()
			second = tickTime.Second()

			// reset metadata
			open = data.Results[i].P
			close = data.Results[i].P
			high = data.Results[i].P
			low = data.Results[i].P
			exchanges = nil
			exchanges = appendStringIfMissing(exchanges, data.Results[i].X)
			size = int64(data.Results[i].S) // init volume for window

			count = 0
		}

	}

	x, _ := json.Marshal(agg)
	fmt.Println(string(x))

	//fmt.Println("done")

}

func appendStringIfMissing(slice []int, i int) []int {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}
