package mydecode_csv_fields

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/processors"
	"github.com/elastic/beats/libbeat/processors/checks"
	jsprocessor "github.com/elastic/beats/libbeat/processors/script/javascript/module/processor"
)

type mydecodeCSVFields struct {
	csvConfig
	fields    map[string]string
	separator rune
	headers   map[string]csvHeader
}

type csvConfig struct {
	Fields           common.MapStr `config:"fields"`
	IgnoreMissing    bool          `config:"ignore_missing"`
	TrimLeadingSpace bool          `config:"trim_leading_space"`
	OverwriteKeys    bool          `config:"overwrite_keys"`
	FailOnError      bool          `config:"fail_on_error"`
	Separator        string        `config:"separator"`
	Headers          common.MapStr `config:"headers`
}

type csvHeader struct {
	custom bool   `config:"custom"`
	header string `config:"string"`
	offset int    `config:"offset"`
	file   `config:"file"`
}

type file struct {
	path string `config:"path"`
}

var (
	defaultCSVConfig = csvConfig{
		Separator:   ",",
		FailOnError: true,
	}

	errFieldAlreadySet = errors.New("field already has a value")
)

func init() {
	processors.RegisterPlugin("mydecode_csv_fields",
		checks.ConfigChecked(MyNewDecodeCSVField,
			checks.RequireFields("fields"),
			checks.AllowedFields("fields", "ignore_missing", "overwrite_keys", "separator", "trim_leading_space", "overwrite_keys", "fail_on_error", "when",
				"headers", "file", "header", "offset", "path")))

	jsprocessor.RegisterPlugin("MyDecodeCSVField", MyNewDecodeCSVField)
}

// NewDecodeCSVField construct a new decode_csv_field processor.
func MyNewDecodeCSVField(c *common.Config) (processors.Processor, error) {
	config := defaultCSVConfig

	err := c.Unpack(&config)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack the decode_csv_field configuration: %s", err)
	}
	if len(config.Fields) == 0 {
		return nil, errors.New("no fields to decode configured")
	}
	f := &mydecodeCSVFields{csvConfig: config}
	// Set separator as rune
	switch runes := []rune(config.Separator); len(runes) {
	case 0:
		break
	case 1:
		f.separator = runes[0]
	default:
		return nil, errors.Errorf("separator must be a single character, got %d in string '%s'", len(runes), config.Separator)
	}
	// Set fields as string -> string
	f.fields = make(map[string]string, len(config.Fields))
	for src, dstIf := range config.Fields.Flatten() {
		dst, ok := dstIf.(string)
		if !ok {
			return nil, errors.Errorf("bad destination mapping for %s: destination field must be string, not %T (got %v)", src, dstIf, dstIf)
		}
		f.fields[src] = dst
	}
	// Set headers as string -> csvHeader
	f.headers = make(map[string]csvHeader, len(config.Headers))
	for src, dstIf := range config.Headers {
		jsonString, _ := json.Marshal(dstIf)
		var dst map[string]interface{}
		json.Unmarshal([]byte(jsonString), &dst)
		var toHeader csvHeader
		if v, found := dst["string"]; found {
			toHeader.header = fmt.Sprintf("%v", v) //v.(string)
		}
		if v, found := dst["offset"]; found {
			toHeader.offset, _ = strconv.Atoi(fmt.Sprintf("%v", v))
		}
		if v, found := dst["custom"]; found {
			toHeader.custom = v.(bool)
		}
		if v, found := dst["file"]; found {
			jsonString, _ = json.Marshal(v)
			json.Unmarshal([]byte(jsonString), &dst)
			if v, found = dst["path"]; found {
				toHeader.file.path = fmt.Sprintf("%v", v)
			}
		}
		f.headers[src] = toHeader
	}
	return f, nil
}

// Run applies the mydecode_csv_field processor to an event.
func (f *mydecodeCSVFields) Run(event *beat.Event) (*beat.Event, error) {
	saved := *event
	if f.FailOnError {
		saved.Fields = event.Fields.Clone()
		saved.Meta = event.Meta.Clone()
	}
	for src, dest := range f.fields {
		if err := f.decodeCSVField(src, dest, event); err != nil && f.FailOnError {
			return &saved, err
		}
	}
	return event, nil
}

func (f *mydecodeCSVFields) decodeCSVField(src, dest string, event *beat.Event) error {
	data, err := event.GetValue(src)
	if err != nil {
		if f.IgnoreMissing && errors.Cause(err) == common.ErrKeyNotFound {
			return nil
		}
		return errors.Wrapf(err, "could not fetch value for field %s", src)
	}

	text, ok := data.(string)
	if !ok {
		return errors.Errorf("field %s is not of string type", src)
	}

	reader := csv.NewReader(strings.NewReader(text))
	reader.Comma = f.separator
	reader.TrimLeadingSpace = f.TrimLeadingSpace
	// LazyQuotes makes the parser more tolerant to bad string formatting.
	reader.LazyQuotes = true

	record, err := reader.Read()
	if err != nil {
		return errors.Wrapf(err, "error decoding CSV from field %s", src)
	}

	if src != dest && !f.OverwriteKeys {
		if _, err = event.GetValue(dest); err == nil {
			return errors.Errorf("target field %s already has a value. Set the overwrite_keys flag or drop/rename the field first", dest)
		}
	}

	/*********** mon code *********************/
	//check if default decode_csv_fields or custom
	if _, exist := f.Headers[src]; exist == false {
		if _, err = event.PutValue(dest, record); err != nil {
			return errors.Wrapf(err, "failed setting field %s", dest)
		}
		return nil
	}

	/*
		priority:
		1- header string in .yml
		2- header in a file conf
		3- header in the file haverested
	*/

	firstLine := ""
	head, _ := f.headers[src]
	//event.PutValue("test", len(f.headers));
	if len(head.header) > 0 {
		//entete ecrit dans le fichier .yml
		firstLine = head.header
	} else {
		str := ""
		if len(head.file.path) > 0 {
			//entete ecrit dans une fichier de conf
			str = head.file.path
		} else {
			//entete ecrit dans la 1ere ligne du fichier ecoute
			//get path file and open file
			path, err := event.GetValue("log.file.path")
			if err != nil {
				return errors.Wrapf(err, "mydecode_csv_fields only works with file, could not fetch value for field log.file.path")
			}
			str = fmt.Sprintf("%v", path)
		}
		if head.offset == 0 {
			head.offset = 1
		}
		file, err := os.Open(str)
		if err != nil {
			return errors.Wrapf(err, "could not open file : log.file.path")
		}
		defer file.Close()

		//read header in file
		scanner := bufio.NewScanner(file)
		for scanner.Scan() && head.offset > 0 {
			firstLine = scanner.Text()
			head.offset--
			if err := scanner.Err(); err != nil {
				return errors.Wrapf(err, "error from scanner.Text() in read file")
			}
		}
		if head.offset != 0 {
			return errors.Wrapf(err, "error: offset too big")
		}
	}
	if text == firstLine {
		return nil
	}
	//get header record
	reader = csv.NewReader(strings.NewReader(firstLine))
	reader.Comma = f.separator
	reader.TrimLeadingSpace = f.TrimLeadingSpace
	// LazyQuotes makes the parser more tolerant to bad string formatting.
	reader.LazyQuotes = true
	headcsv, err := reader.Read()
	if err != nil {
		return errors.Wrapf(err, "error decoding first firstLine")
	}

	//create json object
	mymap := make(map[string]string)
	for i := 0; i < len(headcsv); i++ {
		mymap[headcsv[i]] = record[i]
	}

	//put result in fields dest
	if _, err = event.PutValue(dest, mymap); err != nil {
		return errors.Wrapf(err, "failed setting field %s", dest)
	}
	return nil
}

// String returns a string representation of this processor.
func (f mydecodeCSVFields) String() string {
	json, _ := json.Marshal(f.csvConfig)
	return "decode_csv_field=" + string(json)
}
