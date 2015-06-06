package main

import (
  "encoding/json"
  "fmt"
  "flag"
  "net/http"
  "io/ioutil"
  "github.com/boltdb/bolt"
)


var myDB  *bolt.DB

func mainHandler(w http.ResponseWriter, r *http.Request) {
  var v = map[string]interface{}{
    "size" : myDB.Stats(),
  }
  str, _ := json.Marshal( v )
  fmt.Fprintf(w, string(str))
}

// Store form parameter value of specified key to *val and return true; if key does not exist, set HTTP status 400 and return false.
func Require(w http.ResponseWriter, r *http.Request, key string, val *string) bool {
  *val = r.FormValue(key)
  if *val == "" {
    http.Error(w, fmt.Sprintf("Please pass POST/PUT/GET parameter value of '%s'.", key), 400)
    return false
  }
  return true
}

func docHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Cache-Control", "must-revalidate")
    w.Header().Set("Content-Type", "application/json")

    if (r.Method == "GET") {
        myDB.View(func(tx *bolt.Tx) error {
            b := tx.Bucket([]byte("docs"))
            b.ForEach(func(k, v []byte) error {
                var vdoc map[string]interface{}
                json.Unmarshal(v, &vdoc)
                filter := true
                for fk, fv := range r.URL.Query() {
                    if (vdoc[fk] != fv[0]) {
                        filter = false
                    }
                }
                if (filter) {
                    w.Write( []byte(fmt.Sprintf("{\"%s\":%s}\n", k, v ) ) )
                }
                return nil
            })
            return nil
        })
    } else if (r.Method == "POST") {
        data, err := ioutil.ReadAll(r.Body)
        fmt.Printf("%s\n", data)
        var value map[string]interface{}
        if err == nil && data != nil {
            err = json.Unmarshal(data, &value)
        }

        myDB.Update(func(tx *bolt.Tx) error {
            var bucket = tx.Bucket([]byte("docs"))
            newStr, _ := json.Marshal(value)
            bucket.Put([]byte(value["uuid"].(string)), []byte(newStr))
            return nil
        })
      }
}

func stringInSlice(a string, list []string) bool {
  for _, b := range list {
    if b == a {
      return true
    }
  }
  return false
}

func serverMain(myDBDir string) {

  db, err := bolt.Open(myDBDir, 0644, nil)
  myDB = db
  if err != nil {
    panic(err)
  }

  err = db.Update(func(tx *bolt.Tx) error {
    _, err := tx.CreateBucketIfNotExists([]byte("docs"))
    if err != nil {
        return err
    }
    return nil
  })

  http.HandleFunc("/", mainHandler)
  http.HandleFunc("/api/docs", docHandler)

  http.ListenAndServe(":18888", nil)
  fmt.Printf("Closing Server")
  // Gracefully close database
  if err := myDB.Close(); err != nil {
    panic(err)
  }
}


func main() {
    flag.Parse()
    var db_path = "object_db"
    if (len(flag.Args()) > 0) {
        db_path = flag.Arg(0)
    }
    serverMain(db_path)
}
