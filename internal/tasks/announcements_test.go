package tasks

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hibiken/asynq"
)

// sampleReply mirrors one announcement from docs/examples/disclosure.md
// (SWID, 3 PDF attachments).
func sampleReply() AnnouncementReply {
	return AnnouncementReply{
		Pengumuman: AnnouncementMeta{
			ID2:             "20260808172209-001/CORSEC/SWID/VIII/2026_id-id",
			NoPengumuman:    "001/CORSEC/SWID/VIII/2026",
			TglPengumuman:   "2026-08-08T17:22:09",
			JudulPengumuman: "Laporan Bulanan Registrasi Pemegang Efek",
			KodeEmiten:      "SWID                                                                                                ",
		},
		Attachments: []AnnouncementAttachment{
			{PDFFilename: "c85d17ea16_4908f8efe9.pdf", FullSavePath: "https://www.idx.co.id/StaticData/NewsAndAnnouncement/ANNOUNCEMENTSTOCK/From_EREP/202608/c85d17ea16_4908f8efe9.pdf"},
			{PDFFilename: "a328e5de8c_5e07caa19e.pdf", FullSavePath: "https://www.idx.co.id/StaticData/NewsAndAnnouncement/ANNOUNCEMENTSTOCK/From_EREP/202608/a328e5de8c_5e07caa19e.pdf"},
			{PDFFilename: "a599091a35_36d88c6823.pdf", FullSavePath: "https://www.idx.co.id/StaticData/NewsAndAnnouncement/ANNOUNCEMENTSTOCK/From_EREP/202608/a599091a35_36d88c6823.pdf"},
		},
	}
}

func TestAnnouncementsPayload_Marshal(t *testing.T) {
	p := AnnouncementsPayload{Date: "2026-08-09"}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var got AnnouncementsPayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got.Date != "2026-08-09" {
		t.Errorf("expected date 2026-08-09, got %s", got.Date)
	}
}

func TestEnqueueAnnouncements_TaskTypeAndQueue(t *testing.T) {
	date := time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)

	payload := AnnouncementsPayload{Date: "2026-08-09"}
	raw, _ := json.Marshal(payload)
	task := asynq.NewTask(TypeAnnouncements, raw)

	if task.Type() != TypeAnnouncements {
		t.Errorf("expected type %s, got %s", TypeAnnouncements, task.Type())
	}

	var got AnnouncementsPayload
	if err := json.Unmarshal(task.Payload(), &got); err != nil {
		t.Fatalf("unmarshal task payload: %v", err)
	}
	if got.Date != date.Format("2006-01-02") {
		t.Errorf("expected date %s, got %s", date.Format("2006-01-02"), got.Date)
	}
}

func TestAnnouncementsResponse_Unmarshal(t *testing.T) {
	raw := `{
		"ResultCount": 2,
		"Replies": [
			{
				"pengumuman": {
					"Id2": "20260808172209-001/CORSEC/SWID/VIII/2026_id-id",
					"NoPengumuman": "001/CORSEC/SWID/VIII/2026",
					"TglPengumuman": "2026-08-08T17:22:09",
					"JudulPengumuman": "Laporan Bulanan Registrasi Pemegang Efek",
					"JenisPengumuman": "STOCK",
					"Kode_Emiten": "SWID",
					"Form_Id": "10000"
				},
				"attachments": [
					{"PDFFilename": "c85d17ea16_4908f8efe9.pdf", "FullSavePath": "https://www.idx.co.id/StaticData/NewsAndAnnouncement/ANNOUNCEMENTSTOCK/From_EREP/202608/c85d17ea16_4908f8efe9.pdf"},
					{"PDFFilename": "a328e5de8c_5e07caa19e.pdf", "FullSavePath": "https://www.idx.co.id/StaticData/NewsAndAnnouncement/ANNOUNCEMENTSTOCK/From_EREP/202608/a328e5de8c_5e07caa19e.pdf"}
				]
			},
			{
				"pengumuman": {
					"Id2": "20260808170403-001/MKAP/CORSEC/VIII/2026_id-id",
					"TglPengumuman": "2026-08-08T17:04:03",
					"JudulPengumuman": "Laporan Bulanan Registrasi Pemegang Efek",
					"Kode_Emiten": "MKAP"
				},
				"attachments": [
					{"PDFFilename": "93ef412e76_c58f7d2dcb.pdf", "FullSavePath": "https://www.idx.co.id/StaticData/NewsAndAnnouncement/ANNOUNCEMENTSTOCK/From_EREP/202608/93ef412e76_c58f7d2dcb.pdf"}
				]
			}
		]
	}`

	var resp AnnouncementsResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	if resp.ResultCount != 2 {
		t.Errorf("expected ResultCount 2, got %d", resp.ResultCount)
	}
	if len(resp.Replies) != 2 {
		t.Fatalf("expected 2 replies, got %d", len(resp.Replies))
	}

	first := resp.Replies[0]
	if first.Pengumuman.JudulPengumuman != "Laporan Bulanan Registrasi Pemegang Efek" {
		t.Errorf("unexpected title %q", first.Pengumuman.JudulPengumuman)
	}
	if first.Pengumuman.TglPengumuman != "2026-08-08T17:22:09" {
		t.Errorf("unexpected date %q", first.Pengumuman.TglPengumuman)
	}
	if len(first.Attachments) != 2 {
		t.Fatalf("expected 2 attachments, got %d", len(first.Attachments))
	}
	if first.Attachments[1].FullSavePath != "https://www.idx.co.id/StaticData/NewsAndAnnouncement/ANNOUNCEMENTSTOCK/From_EREP/202608/a328e5de8c_5e07caa19e.pdf" {
		t.Errorf("unexpected pdf url %q", first.Attachments[1].FullSavePath)
	}
}

func TestReplyToDisclosures_MultiAttachment(t *testing.T) {
	rows := replyToDisclosures(sampleReply())

	if len(rows) != 3 {
		t.Fatalf("expected 3 disclosure rows (one per PDF), got %d", len(rows))
	}

	// One row per attachment, attachment_idx 0..2.
	for i, d := range rows {
		if d.AttachmentIdx != int32(i) {
			t.Errorf("row %d: expected attachment_idx %d, got %d", i, i, d.AttachmentIdx)
		}
		if d.PdfURL == "" {
			t.Errorf("row %d: empty pdf_url", i)
		}
		if d.Ticker == nil || *d.Ticker != "SWID" {
			t.Errorf("row %d: expected ticker SWID, got %v", i, d.Ticker)
		}
		if d.Title != "Laporan Bulanan Registrasi Pemegang Efek" {
			t.Errorf("row %d: unexpected title %q", i, d.Title)
		}
		if d.AnnouncementDate.Format("2006-01-02") != "2026-08-08" {
			t.Errorf("row %d: expected announcement date 2026-08-08, got %s", i, d.AnnouncementDate.Format("2006-01-02"))
		}
	}

	if rows[0].PdfURL != "https://www.idx.co.id/StaticData/NewsAndAnnouncement/ANNOUNCEMENTSTOCK/From_EREP/202608/c85d17ea16_4908f8efe9.pdf" {
		t.Errorf("unexpected pdf_url %q", rows[0].PdfURL)
	}
}

func TestReplyToDisclosures_TickerHandling(t *testing.T) {
	// Empty Kode_Emiten (e.g. corporate announcements) -> nil ticker.
	r := sampleReply()
	r.Pengumuman.KodeEmiten = "   "
	rows := replyToDisclosures(r)
	if len(rows) == 0 {
		t.Fatal("expected rows for padded-empty ticker")
	}
	if rows[0].Ticker != nil {
		t.Errorf("expected nil ticker for empty Kode_Emiten, got %q", *rows[0].Ticker)
	}

	// Whitespace padding trimmed.
	r = sampleReply()
	r.Pengumuman.KodeEmiten = "BBCA  "
	rows = replyToDisclosures(r)
	if rows[0].Ticker == nil || *rows[0].Ticker != "BBCA" {
		t.Errorf("expected trimmed ticker BBCA, got %v", rows[0].Ticker)
	}
}

func TestReplyToDisclosures_SkipsEmptyPDFURL(t *testing.T) {
	r := sampleReply()
	r.Attachments[1].FullSavePath = " "
	rows := replyToDisclosures(r)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (one empty URL skipped), got %d", len(rows))
	}
	// attachment_idx still reflects original position: 0, 2.
	if rows[1].AttachmentIdx != 2 {
		t.Errorf("expected attachment_idx 2, got %d", rows[1].AttachmentIdx)
	}
}

func TestReplyToDisclosures_InvalidDate(t *testing.T) {
	r := sampleReply()
	r.Pengumuman.TglPengumuman = "not-a-date"
	if rows := replyToDisclosures(r); rows != nil {
		t.Errorf("expected nil rows for invalid date, got %d", len(rows))
	}
}

func TestMaxAnnouncementDate(t *testing.T) {
	r1 := sampleReply() // 2026-08-08T17:22:09
	r2 := sampleReply()
	r2.Pengumuman.TglPengumuman = "2026-08-09T09:30:00"

	max := maxAnnouncementDate([]AnnouncementReply{r1, r2})
	if max == nil {
		t.Fatal("expected non-nil max date")
	}
	if max.Format("2006-01-02 15:04:05") != "2026-08-09 09:30:00" {
		t.Errorf("expected 2026-08-09 09:30:00, got %s", max.Format("2006-01-02 15:04:05"))
	}

	// All unparseable -> nil.
	r3 := sampleReply()
	r3.Pengumuman.TglPengumuman = "bad"
	if got := maxAnnouncementDate([]AnnouncementReply{r3}); got != nil {
		t.Errorf("expected nil max for unparseable dates, got %v", got)
	}

	// Empty -> nil.
	if got := maxAnnouncementDate(nil); got != nil {
		t.Errorf("expected nil max for empty input, got %v", got)
	}
}

func TestTaskKeyAnnouncements(t *testing.T) {
	key := TaskKey(TypeAnnouncements, "2026-08-09")
	expected := "idx:announcements:2026-08-09"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}
