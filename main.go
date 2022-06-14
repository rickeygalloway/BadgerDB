package main

import (
	"fmt"
	"log"
	"os"

	badger "github.com/dgraph-io/badger/v3"
)

func main() {
	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Transactions
	tran1 := db.NewTransaction(true)
	defer tran1.Discard()

	//insert
	check(tran1.Set([]byte("bKey"), []byte("bValue")))
	check(tran1.Commit()) //Discard is a "rollback"
	fmt.Printf("Inserted key '%s' using tran1.Set\n", "bKey")

	tran1.Delete([]byte("bKey"))
	tran1.Commit()

	//read using another transaction
	tran2 := db.NewTransaction(false)

	entry, err := tran2.Get([]byte("bKey"))
	check(err)
	fmt.Printf("Read Key '%s' using tran2.Get\n", string(entry.Key()))

	N, M := 50000, 1000

	wb := db.NewWriteBatch()
	defer wb.Cancel()
	for i := 0; i < N; i++ {
		check(wb.Set(key(i), val(i)))
	}
	for i := 0; i < M; i++ {
		check(wb.Delete(key(i)))
	}

	check(wb.Flush())

	fmt.Println("Inserted ", N, "Deleted", M)

	err = db.View(func(txn *badger.Txn) error {
		iopt := badger.DefaultIteratorOptions
		itr := txn.NewIterator(iopt)

		defer itr.Close() //iterators must be closed

		i := 0
		for itr.Rewind(); itr.Valid(); itr.Next() {
			i++
		}
		fmt.Println("Read", i, "keys")
		return nil
	})

	fmt.Print("--------------------------DONE--------------------------\n\n\n")

	//get input
	for {
		var choice string
		fmt.Println("Enter your choice")
		fmt.Println("1. Enter value in BadgerDb")
		fmt.Println("2. Display all Records")
		fmt.Println("3. Delete Records")
		fmt.Println("4. Exit")
		fmt.Scanln(&choice)

		switch choice {
		case "1":
			var key, value string
			fmt.Println("Enter Key")
			fmt.Scanln(&key)
			fmt.Println("Enter Value")
			fmt.Scanln(&value)
			InsertRecords(db, key, value)
		case "2":
			DisplayRecords(db)
		case "3":
			var key string
			fmt.Println("Enter Key")
			fmt.Scanln(&key)
			DeleteRecords(db, key)
		default:
			os.Exit(0)
		}
	}
}

func InsertRecords(db *badger.DB, key string, value string) {
	err := db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(value))
		return err
	})

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Record Inserted")
	}
}

func DisplayRecords(db *badger.DB) {
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		fmt.Println(err)
	}
}

func DeleteRecords(db *badger.DB, key string) {
	err := db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if string(k) == key {
				err := txn.Delete(k)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Println("Record Deleted - ", key)
				}
			}
		}
		return nil
	})

	if err != nil {
		fmt.Println(err)
	}
}

func val(i int) []byte {
	return []byte(fmt.Sprintf("%0128d", i))
}

func key(i int) []byte {
	return []byte(fmt.Sprintf("%d", i))
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
