package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"

	"github.com/bluele/gcache"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Mdb ...
type Mdb struct {
	client *mongo.Client
}

func (db *Mdb) connect(ctx context.Context) error {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		return err
	}
	db.client = client
	fmt.Println("Connected To mongodb")
	return nil
}

func (db *Mdb) findByID(ctx context.Context, id string) {
	//col := db.client.Database("test").Collection("fk")
	did, _ := primitive.ObjectIDFromHex(id)
	bm := bson.M{"_id": did}

	col := db.client.Database("test").Collection("fk")
	res := col.FindOne(ctx, bm)
	if res.Err() != nil {
		log.Println("FindEROROR")
	}
	log.Println(res.DecodeBytes())
}

func (db *Mdb) updateByID(ctx context.Context, doc []byte) {
	var bm bson.M

	err := json.Unmarshal(doc, &bm)
	if err != nil {
		log.Println("ERROR query", doc)
	}
	id, ok := bm["a"]
	if !ok {
		log.Println("ERROR, update by bm")
	}
	opts := options.FindOneAndUpdate().SetUpsert(true)
	col := db.client.Database("test").Collection("fk")
	q := bson.M{"a": id}
	d := bson.D{{"$set", bm}}
	res := col.FindOneAndUpdate(ctx, q, d, opts)
	log.Println("UPDATE", q, d)
	log.Println(res.DecodeBytes())
}

//DB mongodb driver
type DB struct {
	DbURI   string
	DbName  string
	ColName string
	cli     *mongo.Client
	col     *mongo.Collection
	gc      gcache.Cache
}

func (db *DB) conn(ctx context.Context) error {

	if db.cli != nil && db.col != nil {
		return nil
	}

	client, err := mongo.NewClient(options.Client().ApplyURI(db.DbURI))
	if err != nil {
		db.cli = nil
		db.col = nil
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		return err
	}
	db.cli = client
	col := client.Database(db.DbName).Collection(db.ColName)
	db.col = col

	return nil
}

//FindOne Find one and return in json
func (db *DB) FindOne(ctx context.Context, q io.Reader, r io.Writer) error {
	err := db.conn(ctx)
	if err != nil {
		return errors.WithMessage(err, "Database Connection ERROR")
	}
	var query, result bson.M
	err = json.NewDecoder(q).Decode(&query)
	if err != nil {
		return errors.WithMessage(err, "Query is not valid json or empty")
	}
	rst := db.col.FindOne(ctx, query)
	err = rst.Err()
	if err != nil {
		return errors.WithMessage(err, "MongoDB FindOne failure")
	}

	err = rst.Decode(&result)
	if err != nil {
		return errors.WithMessage(err, "MongoDB FindOne.Decode failure")
	}
	log.Println(result)

	err = json.NewEncoder(r).Encode(result)
	if err != nil {
		return errors.WithMessage(err, "JSON Encoding ERROR")
	}

	return nil
}

//FindAll find all
func (db *DB) FindAll(ctx context.Context, q io.Reader, r io.Writer) error {
	err := db.conn(ctx)
	if err != nil {
		return errors.WithMessage(err, "Database Connection ERROR")
	}
	var query bson.M
	err = json.NewDecoder(q).Decode(&query)
	if err != nil {
		return errors.WithMessage(err, "Query is not valid json or empty")
	}
	cur, err := db.col.Find(ctx, query)
	if err != nil {
		return errors.WithMessage(err, "MongoDB Find ERROR")
	}

	defer cur.Close(ctx)
	var results []bson.M
	err = cur.All(ctx, &results)
	if err != nil {
		return errors.WithMessage(err, "MongoDB Find.All ERROR")
	}

	err = json.NewEncoder(r).Encode(results)
	if err != nil {
		return errors.WithMessage(err, "JSON Encoding ERROR")
	}

	return nil
}

//Save Save one and return in json
func (db *DB) Save(ctx context.Context, q io.Reader, r io.Writer) error {
	err := db.conn(ctx)
	if err != nil {
		return errors.WithMessage(err, "Database Connection ERROR")
	}
	var query bson.M
	err = json.NewDecoder(q).Decode(&query)
	if err != nil {
		return errors.WithMessage(err, "Query is not valid json or empty")
	}
	ins, err := db.col.InsertOne(ctx, query)
	if err != nil {
		return errors.WithMessage(err, "MongoDB Insert ERROR")
	}
	err = json.NewEncoder(r).Encode(ins.InsertedID)
	if err != nil {
		return errors.WithMessage(err, "JSON Encoding ERROR")
	}

	return nil
}

//Delete Delete one and return in json
func (db *DB) Delete(ctx context.Context, q io.Reader, r io.Writer) error {
	err := db.conn(ctx)
	if err != nil {
		return errors.WithMessage(err, "Database Connection ERROR")
	}
	var query bson.M
	err = json.NewDecoder(q).Decode(&query)
	if err != nil {
		return errors.WithMessage(err, "Query is not valid json or empty")
	}
	del, err := db.col.DeleteOne(ctx, query)
	if err != nil {
		return errors.WithMessage(err, "MongoDB Insert ERROR")
	}
	err = json.NewEncoder(r).Encode(del.DeletedCount)
	if err != nil {
		return errors.WithMessage(err, "JSON Encoding ERROR")
	}

	return nil
}

//FindOneHandle wrapFunction for http
func (db *DB) FindOneHandle(w http.ResponseWriter, r *http.Request) {
	err := db.FindOne(r.Context(), r.Body, w)
	if err != nil {
		http.Error(w, err.Error(), 404)
	}
}

//SaveHandle http wrap
func (db *DB) SaveHandle(w http.ResponseWriter, r *http.Request) {
	err := db.Save(r.Context(), r.Body, w)
	if err != nil {
		http.Error(w, err.Error(), 404)
	}
}

//DeleteHandle http wrap
func (db *DB) DeleteHandle(w http.ResponseWriter, r *http.Request) {
	err := db.Delete(r.Context(), r.Body, w)
	if err != nil {
		http.Error(w, err.Error(), 404)
	}
}

//FindAllHandle http wrap
func (db *DB) FindAllHandle(w http.ResponseWriter, r *http.Request) {
	err := db.FindAll(r.Context(), r.Body, w)

	if err != nil {
		http.Error(w, err.Error(), 404)
	}
}

type ctxKey string

const (
	keyToken ctxKey = "token"
)

//AuthChecker chi middleware
func (db *DB) AuthChecker(next http.Handler) http.Handler {
	fn := func(resp http.ResponseWriter, req *http.Request) {
		cookie, _ := req.Cookie("XTOKEN")
		token := cookie.Value
		if token == "" {
			http.Error(resp, "No permission0", 403)
			return
		}
		log.Println("db.has " + token + " = " + strconv.FormatBool(db.gc.Has(token)))
		if !db.gc.Has(token) {
			http.Error(resp, "No permission1", 403)
			return
		}
		ctx := context.WithValue(req.Context(), keyToken, token)
		next.ServeHTTP(resp, req.WithContext(ctx))
	}
	return http.HandlerFunc(fn)
}

//Auth http auth
func (db *DB) Auth(w http.ResponseWriter, r *http.Request) {
	token := strconv.FormatInt(rand.Int63(), 16)
	cookie := http.Cookie{
		Name:    "XTOKEN",
		Value:   token,
		Expires: time.Now().Add(time.Hour),
		Path:    "/",
	}
	http.SetCookie(w, &cookie)
	// r.AddCookie(&cookie)
	w.Write([]byte(`{"err":0, "msg": "OK"}` + token))
	log.Println("cache.set " + token)
	db.gc.Set(token, r.RemoteAddr)
}

//Stat http stat
func (db *DB) Stat(w http.ResponseWriter, r *http.Request) {
	fn := func(c interface{}) {
		w.Write([]byte(fmt.Sprintf("%v", c)))
		w.Write([]byte("\r\n"))
	}
	fn(db.gc.GetALL(false))
	fn(r.RemoteAddr)
	fn(r.RequestURI)
	fn(r.URL.RawQuery)

}

func main() {
	dbx := DB{DbURI: "mongodb://localhost:27017", DbName: "test", ColName: "test1"}
	dbx.gc = gcache.New(10000).ARC().Build()

	err := dbx.gc.Set("A", "B")
	if err != nil {
		log.Println(err)
	}
	log.Println(dbx.gc.Has("A"))
	val, err := dbx.gc.Get("A")
	log.Println(val, err)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Get("/auth", dbx.Auth)
	r.Get("/stat", dbx.Stat)

	r.With(dbx.AuthChecker).Route("/mdb", func(r chi.Router) {
		r.Post("/find", dbx.FindOneHandle)
		r.Post("/save", dbx.SaveHandle)
		r.Post("/delete", dbx.DeleteHandle)
		r.Post("/findall", dbx.FindAllHandle)

	})
	http.ListenAndServe(":10080", r)
}
