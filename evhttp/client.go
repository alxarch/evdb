package evhttp

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/alxarch/httperr"
	errors "golang.org/x/xerrors"
)

// HTTPClient does HTTP requests
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

func sendJSON(ctx context.Context, c HTTPClient, req *http.Request, x interface{}) error {
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	if c == nil {
		c = http.DefaultClient
	}
	res, err := c.Do(req)
	if err != nil {
		return err
	}
	if httperr.IsError(res.StatusCode) {
		return httperr.FromResponse(res)
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return errors.Errorf(`Failed to read response: %s`, err)
	}
	return json.Unmarshal(data, x)

}
