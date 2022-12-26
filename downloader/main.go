//
// download grouped data
//   range over each ticker
//     download all trades
//     download all quotes
//     combine trades + quotes into single struct
//     sort combined trades + quotes by time
//     write into compressed gob + lz4
//
package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"time"

	"github.com/pierrec/lz4"
)

// Tickers - https://polygon.io/docs/stocks/get_v3_reference_tickers
type Tickers struct {
	Results []struct {
		Ticker          string    `json:"ticker"`                     // "CPK"
		Name            string    `json:"name"`                       // "Chesapeake Utilities"
		Market          string    `json:"market"`                     // "stocks"
		Locale          string    `json:"locale"`                     // "us"
		PrimaryExchange string    `json:"primary_exchange"`           // "XNYS"
		Type            string    `json:"type"`                       // "CS"
		Active          bool      `json:"active"`                     //  true
		CurrencyName    string    `json:"currency_name"`              // "usd"
		Cik             string    `json:"cik,omitempty"`              // "0000019745"
		CompositeFigi   string    `json:"composite_figi,omitempty"`   // "BBG000G4GKH3"
		ShareClassFigi  string    `json:"share_class_figi,omitempty"` // "BBG001SCP5R2"
		LastUpdatedUtc  time.Time `json:"last_updated_utc"`           // "2021-03-04T00:00:00Z
	} `json:"results"`
	Status    string `json:"status"`
	RequestID string `json:"request_id"`
	Count     int    `json:"count"`
	NextURL   string `json:"next_url"`
}

// Trades - https://polygon.io/docs/stocks/get_v3_trades__stockticker
type Trades struct {
	Results []struct {
		Conditions           []int   `json:"conditions"`              // A list of condition codes.
		Correction           int     `json:"correction"`              // The trade correction indicator.
		Exchange             int     `json:"exchange"`                // The exchange ID. See Exchanges for Polygon.io's mapping of exchange IDs.
		ID                   string  `json:"id"`                      // The Trade ID which uniquely identifies a trade.
		ParticipantTimestamp int64   `json:"participant_timestamp"`   // The nanosecond accuracy Participant/Exchange Unix Timestamp.
		Price                float64 `json:"price"`                   // The price of the trade.
		SequenceNumber       int     `json:"sequence_number"`         // The sequence number represents the sequence in which trade events happened.
		SipTimestamp         int64   `json:"sip_timestamp"`           // The nanosecond accuracy SIP Unix Timestamp.
		Size                 int64   `json:"size"`                    // The size of a trade (also known as volume).
		Tape                 int     `json:"tape"`                    // There are 3 tapes which define which exchange the ticker is listed on.
		TrfID                int     `json:"trf_id,omitempty"`        // The ID for the Trade Reporting Facility where the trade took place.
		TrfTimestamp         int64   `json:"trf_timestamp,omitempty"` // The nanosecond accuracy TRF (Trade Reporting Facility) Unix Timestamp.
	} `json:"results"`
	Status    string `json:"status"`
	RequestID string `json:"request_id"`
	NextURL   string `json:"next_url"`
}

// Quotes - https://polygon.io/docs/stocks/get_v3_quotes__stockticker
type Quotes struct {
	Results []struct {
		AskExchange          int     `json:"ask_exchange"`          // The ask exchange ID.
		AskPrice             float64 `json:"ask_price"`             // The ask price.
		AskSize              int     `json:"ask_size"`              // The ask size.
		BidExchange          int     `json:"bid_exchange"`          // The bid exchange ID.
		BidPrice             float64 `json:"bid_price"`             // The bid price.
		BidSize              int     `json:"bid_size"`              // The bid size.
		Conditions           []int   `json:"conditions"`            // A list of condition codes.
		Indicators           []int   `json:"indicators"`            // The indicators.
		ParticipantTimestamp int64   `json:"participant_timestamp"` // The nanosecond accuracy Participant/Exchange Unix Timestamp.
		SequenceNumber       int     `json:"sequence_number"`       // The sequence number represents the sequence in which quote events happened.
		SipTimestamp         int64   `json:"sip_timestamp"`         // The nanosecond accuracy SIP Unix Timestamp.
		Tape                 int     `json:"tape"`                  // There are 3 tapes which define which exchange the ticker is listed on.
	} `json:"results"`
	Status    string `json:"status"`
	RequestID string `json:"request_id"`
	NextURL   string `json:"next_url"`
}

// TradesQuotesCombined - trades + quotes in one combined stream
type TradesQuotesCombined struct {
	Sym string  // The ticker symbol for the given stock
	EV  string  // The event type (T/Q)
	T   int64   // The Timestamp in Unix MS
	TF  int64   // The nanosecond accuracy TRF(Trade Reporting Facility) Unix Timestamp. This is the timestamp of when the trade reporting facility received this message.
	TQ  int     // The sequence number representing the sequence in which trade events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11).
	TY  int64   // The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the quote was actually generated at the exchange.
	TE  int     // The trade correction indicator.
	TI  string  // The trade ID // int64?
	TP  float64 // Trade price
	TS  int64   // Trade size
	TC  []int   // Trade condition
	TX  int     // Trade exchange ID
	TR  int     // The ID for the Trade Reporting Facility where the trade took place.
	TZ  int     // Trade tape. (1 = NYSE, 2 = AMEX, 3 = Nasdaq)
	QF  int64   // The nanosecond accuracy TRF(Trade Reporting Facility) Unix Timestamp. This is the timestamp of when the trade reporting facility received this message.
	QQ  int     // The sequence number represents the sequence in which message events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11).
	QY  int64   // The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the quote was actually generated at the exchange.
	QI  []int   // The indicators. For more information, see our glossary of Conditions and Indicators.
	BX  int     // The bid exchange ID
	BP  float64 // The bid price
	BS  int     // The bid size. This represents the number of round lot orders at the given bid price. The normal round lot size is 100 shares. A bid size of 2 means there are 200 shares for purchase at the given bid price
	AX  int
	AP  float64
	AS  int
	BSC []int // The condition
	BSZ int   // The tape. (1 = NYSE, 2 = AMEX, 3 = Nasdaq)
}

var errorCount int
var APIKEY = ""
var OUTPUTDIR = "/scratch/historical/"

func main() {

	days := []string{

		/*

			"2022-07-05", "2022-07-06", "2022-07-07", "2022-07-08", //"2022-07-04", Independence Day
			"2022-07-11", "2022-07-12", "2022-07-13", "2022-07-14", "2022-07-15",
			"2022-07-18", "2022-07-19", "2022-07-20", "2022-07-21", "2022-07-22",
			"2022-07-25", "2022-07-26", "2022-07-27", "2022-07-28", "2022-07-29",
			"2022-08-01", "2022-08-02", "2022-08-03", "2022-08-04", "2022-08-05",
			"2022-08-08", "2022-08-09", "2022-08-10", "2022-08-11", "2022-08-12",
			"2022-08-15", "2022-08-16", "2022-08-17", "2022-08-18", "2022-08-19",
			"2022-08-22", "2022-08-23", "2022-08-24", "2022-08-25", "2022-08-26",
			"2022-08-29", "2022-08-30", "2022-08-31", "2022-09-01", "2022-09-02",
			"2022-09-06", "2022-09-07", "2022-09-08", "2022-09-09", // "2022-09-05", Labor Day
			"2022-09-12", "2022-09-13", "2022-09-14", "2022-09-15", "2022-09-16",
			"2022-09-19", "2022-09-20", "2022-09-21", "2022-09-22", "2022-09-23",
			"2022-09-26", "2022-09-27", "2022-09-28", "2022-09-29", "2022-09-30",
		*/
		/*
			"2022-10-03", "2022-10-04", "2022-10-05", "2022-10-06", "2022-10-07",
			"2022-10-10", "2022-10-11", "2022-10-12", "2022-10-13", "2022-10-14",
			"2022-10-17", "2022-10-18", "2022-10-19", "2022-10-20", "2022-10-21",
			"2022-10-24", "2022-10-25", "2022-10-26", "2022-10-27", "2022-10-28",
			"2022-10-31", "2022-11-01", "2022-11-02", "2022-11-03", "2022-11-04",
			"2022-11-07", "2022-11-08", "2022-11-09", "2022-11-10", "2022-11-11",
			"2022-11-14", "2022-11-15", "2022-11-16", "2022-11-17", "2022-11-18",
			"2022-11-21", "2022-11-22", "2022-11-23", "2022-11-24", "2022-11-25",
			"2022-11-28", "2022-11-29", "2022-11-30", "2022-12-01", "2022-12-02",
			"2022-12-05", "2022-12-06", "2022-12-07", "2022-12-08", "2022-12-09",
			"2022-12-12", "2022-12-13", "2022-12-14", "2022-12-15", "2022-12-16",
			"2022-12-19", "2022-12-20", "2022-12-21", "2022-12-22", "2022-12-23",
		*/

		"2022-12-23",
	}

	for _, day := range days {

		t := day
		tx := day

		fmt.Println("processing:", t)

		// chunk
		var offset int     // results offset
		var nextURL string // next results set url

		// init url
		tickersURL := fmt.Sprintf("https://api.polygon.io/v3/reference/tickers?market=stocks&type=CS&date=%v&active=true&sort=ticker&order=asc&limit=1000&apiKey=%v", tx, APIKEY)

		// total results
		var tresults Tickers

		// loop to pull down all results
		for {

			// per request results
			var trequest Tickers

			// do we need to pull down additional tickers?
			if nextURL != "" {
				tickersURL = fmt.Sprintf("https://api.polygon.io%v&apiKey=%v", nextURL, APIKEY)
			}

			//fmt.Println(tickersURL)

			tresp, terr := http.Get(tickersURL)
			if terr != nil {
				log.Fatalln(terr)
			}

			tbody, terr := ioutil.ReadAll(tresp.Body)
			if terr != nil {
				log.Fatalln(terr)
			}

			// throw error on non-200 response
			if tresp.StatusCode != 200 {
				fmt.Println("HTTP Response Status:", tresp.StatusCode, tickersURL, string(tbody))
				errorCount++
				// dump headers
				for name, values := range tresp.Header {
					for _, value := range values {
						fmt.Println(name, value)
					}
				}
				panic("stopping now")
			}

			json.Unmarshal(tbody, &trequest)

			// append results
			for _, record := range trequest.Results {
				tresults.Results = append(tresults.Results, record)
			}

			//fmt.Printf("found %v results at timestamp %v\n", len(trequest.Results), offset)

			// do we need to make another request?
			if trequest.NextURL != "" {

				// add up how man records we have seen
				offset = offset + len(trequest.Results)

				// we need to parse the url path only since we're getting a weird 443 port duplicated error
				u, err := url.Parse(trequest.NextURL)
				if err != nil {
					panic(err)
				}

				nextURL = fmt.Sprintf("%v", u.RequestURI())

			} else {

				// looks like we're at the end of the results
				break
			}

		}

		fmt.Printf("seeding trade/quotes with %v stocks\n", len(tresults.Results))

		// mkdir path
		err := os.MkdirAll(OUTPUTDIR+t, 0755) // mkdir 2021-10-11
		if err != nil {
			log.Fatalln(err)
		}

		// http://jmoiron.net/blog/limiting-concurrency-in-go/
		concurrency := 50 // 150
		sem := make(chan bool, concurrency)

		symbols := []string{}
		for _, ticker := range tresults.Results {
			symbols = append(symbols, ticker.Ticker)
		}

		// range over all tickers
		for _, symbol := range symbols {

			sem <- true
			go func(symbol string) {
				defer func() { <-sem }()

				// store all trades and quotes
				var tqcombined []TradesQuotesCombined

				//
				// trades
				//
				//var trades Trades

				// fetch trades chunk
				var tradeOffset int     // results offset
				var tradeNextURL string // next results set url

				// url
				tradesURL := fmt.Sprintf("https://api.polygon.io/v3/trades/%v?timestamp=%v&limit=50000&apiKey=%v", symbol, t, APIKEY)

				// loop to pull down all results
				for {

					// per request results
					var request Trades

					if tradeNextURL != "" {
						tradesURL = fmt.Sprintf("https://api.polygon.io%v&apiKey=%v", tradeNextURL, APIKEY)
					}

					//fmt.Println(tradesURL)

					resp, err := http.Get(tradesURL)
					if err != nil {
						log.Fatalln(err)
					}

					body, err := ioutil.ReadAll(resp.Body)
					if err != nil {
						log.Fatalln(err)
					}

					// throw error on non-200 response
					if resp.StatusCode != 200 {
						fmt.Println("HTTP Response Status:", resp.StatusCode, tradesURL, string(body))
						errorCount++
						// dump headers
						for name, values := range resp.Header {
							for _, value := range values {
								fmt.Println(name, value)
							}
						}
						panic("stopping now")
					}

					json.Unmarshal(body, &request)

					// append to trades
					//for _, v := range request.Results {
					//	trades.Results = append(trades.Results, v)
					//}

					// 50k pagination logic issue
					if len(request.Results) == 50000 && request.NextURL == "" {
						fmt.Println("possible 50k bug issue on trades: ", tradesURL)

						// dump headers
						for name, values := range resp.Header {
							for _, value := range values {
								fmt.Println(name, value)
							}
						}

					}

					// append results
					for i := range request.Results {

						var v TradesQuotesCombined

						v.Sym = symbol                                 // The ticker symbol for the given stock
						v.EV = "T"                                     // The event type (T/Q)
						v.T = request.Results[i].SipTimestamp          // The Timestamp in Unix MS
						v.TF = request.Results[i].TrfTimestamp         // The nanosecond accuracy TRF(Trade Reporting Facility) Unix Timestamp. This is the timestamp of when the trade reporting facility received this message.
						v.TQ = request.Results[i].SequenceNumber       // The sequence number representing the sequence in which trade events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11).
						v.TY = request.Results[i].ParticipantTimestamp // The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the quote was actually generated at the exchange.
						v.TC = request.Results[i].Conditions           // Trade condition
						v.TE = request.Results[i].Correction           // The trade correction indicator.
						v.TI = request.Results[i].ID                   // The trade ID
						v.TP = request.Results[i].Price                // Trade price
						v.TR = request.Results[i].TrfID                // The ID for the Trade Reporting Facility where the trade took place.
						v.TS = request.Results[i].Size                 // Trade size
						v.TX = request.Results[i].Exchange             // Trade exchange ID
						v.TZ = request.Results[i].Tape                 // Trade tape. (1 = NYSE, 2 = AMEX, 3 = Nasdaq)

						tqcombined = append(tqcombined, v)

					}

					// do we need to make another request?
					if request.NextURL != "" {

						// add up how man records we have seen
						tradeOffset = tradeOffset + len(request.Results)

						// we need to parse the url path only since we're getting a weird 443 port duplicated error
						u, err := url.Parse(request.NextURL)
						if err != nil {
							panic(err)
						}

						tradeNextURL = fmt.Sprintf("%v", u.RequestURI())

					} else {

						// looks like we're at the end of the results
						break
					}

				}

				//
				// quotes
				//

				var quoteOffset int     // results offset
				var quoteNextURL string // next results set url

				// url
				quotesURL := fmt.Sprintf("https://api.polygon.io/v3/quotes/%v?timestamp=%v&limit=50000&apiKey=%v", symbol, t, APIKEY)

				// loop to pull down all results
				for {

					// per request results
					var qrequest Quotes

					if quoteNextURL != "" {
						quotesURL = fmt.Sprintf("https://api.polygon.io%v&apiKey=%v", quoteNextURL, APIKEY)
					}

					//fmt.Println(quotesURL)

					resp, err := http.Get(quotesURL)
					if err != nil {
						log.Fatalln(err)
					}

					body, err := ioutil.ReadAll(resp.Body)
					if err != nil {
						log.Fatalln(err)
					}

					// throw error on non-200 response
					if resp.StatusCode != 200 {
						fmt.Println("HTTP Response Status:", resp.StatusCode, quotesURL, string(body))
						errorCount++
						// dump headers
						for name, values := range resp.Header {
							for _, value := range values {
								fmt.Println(name, value)
							}
						}
						panic("stopping now")
					}

					json.Unmarshal(body, &qrequest)

					// 50k pagination logic issue
					if len(qrequest.Results) == 50000 && qrequest.NextURL == "" {
						fmt.Println("possible 50k bug issue on quotes: ", quotesURL)

						// dump headers
						for name, values := range resp.Header {
							for _, value := range values {
								fmt.Println(name, value)
							}
						}

					}

					// append results
					for i := range qrequest.Results {

						var v TradesQuotesCombined

						v.Sym = symbol                                  // The ticker symbol for the given stock
						v.EV = "Q"                                      // The event type (T/Q)
						v.T = qrequest.Results[i].SipTimestamp          // The Timestamp in Unix MS
						v.QQ = qrequest.Results[i].SequenceNumber       // The sequence number represents the sequence in which message events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11).
						v.QY = qrequest.Results[i].ParticipantTimestamp // The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the quote was actually generated at the exchange.
						v.QI = qrequest.Results[i].Indicators           // The indicators. For more information, see our glossary of Conditions and Indicators.
						v.BX = qrequest.Results[i].BidExchange          // The bid exchange ID
						v.BP = qrequest.Results[i].BidPrice             // The bid price
						v.BS = qrequest.Results[i].BidSize              // The bid size. This represents the number of round lot orders at the given bid price. The normal round lot size is 100 shares. A bid size of 2 means there are 200 shares for purchase at the given bid price
						v.AX = qrequest.Results[i].AskExchange          //
						v.AP = qrequest.Results[i].AskPrice             //
						v.AS = qrequest.Results[i].AskSize              //
						v.BSC = qrequest.Results[i].Conditions          // The condition
						v.BSZ = qrequest.Results[i].Tape                // The tape. (1 = NYSE, 2 = AMEX, 3 = Nasdaq)

						tqcombined = append(tqcombined, v)

					}

					// do we need to make another request?
					if qrequest.NextURL != "" {

						// add up how man records we have seen
						quoteOffset = quoteOffset + len(qrequest.Results)

						// we need to parse the url path only since we're getting a weird 443 port duplicated error
						u, err := url.Parse(qrequest.NextURL)
						if err != nil {
							panic(err)
						}

						quoteNextURL = fmt.Sprintf("%v", u.RequestURI())

					} else {

						// looks like we're at the end of the results
						break
					}

				}

				// sort
				sort.SliceStable(tqcombined, func(i, j int) bool {
					return tqcombined[i].T < tqcombined[j].T
				})

				//fmt.Printf("%v - writing %v with %v records\n", t, symbol, len(tqcombined))

				// gob encoding
				buf := new(bytes.Buffer)
				enc := gob.NewEncoder(buf)
				enc.Encode(tqcombined)

				zw := lz4.NewWriter(nil)
				zfilename := fmt.Sprintf(OUTPUTDIR+t+"/%v-%v.gob.lz4", symbol, t)
				zfile, _ := os.Create(zfilename)
				//zfile, err := os.OpenFile(zfilename, os.O_RDWR, 644)
				if err != nil {
					fmt.Println(err)
				}
				zw.Reset(zfile)

				//_, err = io.Copy(zw, zrfile)
				_, err = io.Copy(zw, buf)
				if err != nil {
					fmt.Println(err)
				}

				for _, c := range []io.Closer{zw, zfile} {
					err := c.Close()
					if err != nil {
						fmt.Println(err)
					}
				}

			}(symbol)

		} // end range

		for i := 0; i < cap(sem); i++ {
			sem <- true
		}

	} // end range days

	fmt.Println("error count:", errorCount)

}
