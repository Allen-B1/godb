package db

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
)

type Doc struct {
	file string
}

func New(file string) *Doc {
	return &Doc{file: file}
}

func (d *Doc) Ref(key string) Ref {
	return Ref{doc: d, keys: []string{key}}
}

func (d *Doc) String() string {
	return d.file
}

func (d *Doc) read() (map[string]interface{}, error) {
	body, err := os.ReadFile(d.file)
	if err != nil {
		return nil, err
	}

	m := make(map[string]interface{})
	err = json.Unmarshal(body, &m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (d *Doc) write(data map[string]interface{}) error {
	f, err := os.Create(d.file)
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := json.Marshal(data)
	if err != nil {
		return err
	}

	f.Write(b)
	return nil
}

type Ref struct {
	doc  *Doc
	keys []string
}

func (r Ref) Get() (interface{}, error) {
	m, err := r.doc.read()
	if err != nil {
		log.Println(err)
		return nil, ErrNotExist
	}

	return getChild(m, r.keys, false)
}

func (r Ref) Set(v interface{}) Transaction {
	return &transactionSet{Ref: r, value: v}
}

func (r Ref) Remove() Transaction {
	return transactionRemove{Ref: r}
}

func (r Ref) Update(f func(interface{}) interface{}) Transaction {
	return transactionUpdate{Ref: r, f: f}
}

func (r Ref) Ref(key string) Ref {
	keys := []string(nil)
	keys = append(keys, r.keys...)
	keys = append(keys, key)
	return Ref{doc: r.doc, keys: keys}
}

var (
	ErrNotExist = fmt.Errorf("key does not exist")
)

func getChild(data map[string]interface{}, keys []string, write bool) (interface{}, error) {
	if len(keys) == 0 {
		return data, nil
	}

	m := data
	for i, key := range keys {
		n, ok := m[key]
		if !ok {
			if write {
				n = make(map[string]interface{})
				m[key] = n
			} else {
				return nil, ErrNotExist
			}
		}
		if i == len(keys)-1 {
			return n, nil
		}

		m, ok = n.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("access of '%s': value at '%s' is %s, not object", strings.Join(keys, "."), strings.Join(keys[:i+1], "."), reflect.TypeOf(n).Name())
		}
	}

	panic("unreachable")
}

type Transaction interface {
	Apply(root map[string]interface{}) error
	Doc() *Doc
}

type transactionSet struct {
	Ref
	value interface{}
}

func (t *transactionSet) Doc() *Doc {
	return t.doc
}

func (t *transactionSet) Apply(root map[string]interface{}) error {
	n, err := getChild(root, t.keys[:len(t.keys)-1], true)
	if err != nil {
		return err
	}
	m, ok := n.(map[string]interface{})
	if !ok {
		return fmt.Errorf("access of '%s': value at '%s' is not a json object", strings.Join(t.keys, "."), strings.Join(t.keys[:len(t.keys)-1], "."))
	}
	m[t.keys[len(t.keys)-1]] = t.value
	return nil
}

type transactionCombine []Transaction

func (t transactionCombine) Apply(root map[string]interface{}) error {
	for _, tr := range t {
		err := tr.Apply(root)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t transactionCombine) Doc() *Doc {
	return t[0].Doc()
}

func tryAll(transactions ...Transaction) (Transaction, error) {
	if len(transactions) == 0 {
		return nil, fmt.Errorf("no transactions specified in db.Update")
	}

	doc := transactions[0].Doc()
	for _, transaction := range transactions[1:] {
		if transaction.Doc() != doc {
			return nil, fmt.Errorf("all transactions must be from the same document")
		}
	}

	return transactionCombine(transactions), nil
}

func All(transactions ...Transaction) Transaction {
	t, err := tryAll(transactions...)
	if err != nil {
		panic(err)
	}
	return t
}

type transactionRemove struct {
	Ref
}

func (t transactionRemove) Apply(root map[string]interface{}) error {
	n, err := getChild(root, t.keys[:len(t.keys)-1], true)
	if err != nil {
		return err
	}
	m, ok := n.(map[string]interface{})
	if !ok {
		return fmt.Errorf("access of '%s': value at '%s' is not a json object", strings.Join(t.keys, "."), strings.Join(t.keys[:len(t.keys)-1], "."))
	}
	delete(m, t.keys[len(t.keys)-1])
	return nil
}

func (t transactionRemove) Doc() *Doc {
	return t.doc
}

type transactionUpdate struct {
	Ref
	f func(interface{}) interface{}
}

func (t transactionUpdate) Apply(root map[string]interface{}) error {
	n, err := getChild(root, t.keys[:len(t.keys)-1], true)
	if err != nil {
		return err
	}
	m, ok := n.(map[string]interface{})
	if !ok {
		return fmt.Errorf("access of '%s': value at '%s' is not a json object", strings.Join(t.keys, "."), strings.Join(t.keys[:len(t.keys)-1], "."))
	}
	m[t.keys[len(t.keys)-1]] = t.f(m[t.keys[len(t.keys)-1]])
	return nil
}

func (t transactionUpdate) Doc() *Doc {
	return t.doc
}

func Do(t Transaction) error {
	m, err := t.Doc().read()
	if err != nil {
		log.Println(err)
		m = make(map[string]interface{})
	}

	err = t.Apply(m)
	if err != nil {
		return err
	}

	t.Doc().write(m)
	return nil
}
