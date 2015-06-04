package main

import (
  "encoding/json"
  "fmt"
  "flag"
  "github.com/HouzuoGuo/tiedot/db"
//  "github.com/HouzuoGuo/tiedot/dberr"
  "net/http"
  "strconv"
  "io/ioutil"
)


var myDB  *db.DB

func mainHandler(w http.ResponseWriter, r *http.Request) {
  myDocs := myDB.Use("docs")
  var v = map[string]interface{}{
    "doc_count" : myDocs.ApproxDocCount(),
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
  dbcol := myDB.Use("docs")
  if dbcol == nil {
      http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", "docs"), 400)
      return
  }

  if (r.Method == "GET") {
      w.Header().Set("Cache-Control", "must-revalidate")
      w.Header().Set("Content-Type", "application/json")
      /*
      var col, page, total string
      if !Require(w, r, "page", &page) {
        return
      }
      if !Require(w, r, "total", &total) {
        return
      }
      totalPage, err := strconv.Atoi(total)
      if err != nil || totalPage < 1 {
        http.Error(w, fmt.Sprintf("Invalid total page number '%v'.", totalPage), 400)
        return
      }
      pageNum, err := strconv.Atoi(page)
      if err != nil || pageNum < 0 || pageNum >= totalPage {
        http.Error(w, fmt.Sprintf("Invalid page number '%v'.", page), 400)
        return
      }
      dbcol := myDB.Use("docs")
      if dbcol == nil {
        http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
        return
      }
      docs := make(map[string]interface{})
      dbcol.ForEachDocInPage(pageNum, totalPage, func(id int, doc []byte) bool {
        var docObj map[string]interface{}
        if err := json.Unmarshal(doc, &docObj); err == nil {
          docs[strconv.Itoa(id)] = docObj
        }
        return true
      })
      resp, err := json.Marshal(docs)
      if err != nil {
        http.Error(w, fmt.Sprint(err), 500)
        return
      }
      w.Write(resp)
      */

      docs := make(map[string]interface{})
      dbcol.ForEachDoc(func(id int, doc []byte) bool {
        var docObj map[string]interface{}
        if err := json.Unmarshal(doc, &docObj); err == nil {
          docs[strconv.Itoa(id)] = docObj
        }
        return true
      })
      resp, err := json.Marshal(docs)
      if err != nil {
        http.Error(w, fmt.Sprint(err), 500)
        return
      }
      w.Write(resp)
  } else if (r.Method == "POST") {
    data, err := ioutil.ReadAll(r.Body)
    fmt.Printf("%s\n", data)
    var value map[string]interface{}
    if err == nil && data != nil {
        err = json.Unmarshal(data, &value)
    }
    fmt.Printf("%s\n", value)
    //docID := db.StrHash(value["uuid"].(string))
    //err = dbcol.Update(docID, value)
    docID, err := dbcol.Insert(value)
    if err != nil {
        panic(err)
    }
    var v = map[string]interface{}{
      "_id" : docID,
    }
    str, _ := json.Marshal( v )
    fmt.Fprintf(w, string(str))
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

  db, err := db.OpenDB(myDBDir)
  myDB = db
  if err != nil {
    panic(err)
  }

  if (!stringInSlice("docs", myDB.AllCols())) {
    if err := myDB.Create("docs"); err != nil {
      panic(err)
    }
  }

  docs := myDB.Use("docs")
  fmt.Printf("Doc count: %d\n", docs.ApproxDocCount() )

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
