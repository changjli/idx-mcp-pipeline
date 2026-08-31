package tasks

import (
	"net/http"
	"strings"
	"testing"

	"github.com/hibiken/asynq"
)

// httpClientFetcher adapts a plain *http.Client to the extract.PDFFetcher seam
// so tests can reuse httptest servers without a real upstream. The fetch
// helpers themselves are tested in internal/extract; this adapter stays here
// for the DB-verify runner tests.
type httpClientFetcher struct{ c *http.Client }

func (f httpClientFetcher) GetStream(url string, extraHeaders map[string]string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	return f.c.Do(req)
}

// fakeEnqueuer captures the task + options it was called with (mirrors the
// pipeline package's fake so enqueue options are assertable cross-package).
type fakeEnqueuer struct {
	tasks []fakeEnqueued
}

type fakeEnqueued struct {
	typ  string
	body []byte
	opts []asynq.Option
}

func (f *fakeEnqueuer) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	f.tasks = append(f.tasks, fakeEnqueued{typ: task.Type(), body: task.Payload(), opts: opts})
	return &asynq.TaskInfo{}, nil
}

func optStrings(opts []asynq.Option) string {
	var sb strings.Builder
	for _, o := range opts {
		sb.WriteString(o.String())
		sb.WriteString("\n")
	}
	return sb.String()
}

// TestEnqueueExtractDisclosure_NoAsynqRetry locks the single-owner retry
// decision (issue 06): asynq retry is disabled so the self-managed
// extractRetryDelays ladder is the only retry clock on the task.
func TestEnqueueExtractDisclosure_NoAsynqRetry(t *testing.T) {
	enq := &fakeEnqueuer{}
	if _, err := EnqueueExtractDisclosure(enq, 42); err != nil {
		t.Fatalf("EnqueueExtractDisclosure: %v", err)
	}
	got := enq.tasks[0]
	if got.typ != TypeExtractDisclosure {
		t.Errorf("expected task type %q, got %q", TypeExtractDisclosure, got.typ)
	}
	opts := optStrings(got.opts)
	if !strings.Contains(opts, "MaxRetry(0)") {
		t.Errorf("expected MaxRetry(0) (asynq retry disabled), got:\n%s", opts)
	}
}
