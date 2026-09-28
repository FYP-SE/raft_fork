package raft

import "testing"

// DESIGN_UPDATE.md D7: the leader hands over when any single signal is
// critical for DegradeWindow intervals, even if its weighted score is still
// above HandoverThreshold.
func TestGracefulHandover_CriticalSignalTriggers(t *testing.T) {
	r, v := newHandoverTestRaft(t, 3, 10)
	setEligibleHeir(r, 2, 255)
	v.Set(200)          // weighted score healthy (> HandoverThreshold 64)...
	v.SetCritical(true) // ...but one signal critical

	for i := 0; i < 2; i++ {
		r.maybeGracefulHandover()
		if r.leadTransferee != None {
			t.Fatalf("handover after %d intervals, want only after DegradeWindow=3", i+1)
		}
	}
	r.maybeGracefulHandover()
	if r.leadTransferee != 2 {
		t.Fatalf("leadTransferee = %d, want 2 after a critical signal for DegradeWindow", r.leadTransferee)
	}
}

func TestGracefulHandover_CriticalClearsResetsWindow(t *testing.T) {
	r, v := newHandoverTestRaft(t, 3, 10)
	setEligibleHeir(r, 2, 255)
	v.Set(200)
	v.SetCritical(true)
	r.maybeGracefulHandover()
	r.maybeGracefulHandover()
	v.SetCritical(false) // recovered
	r.maybeGracefulHandover()
	v.SetCritical(true)
	r.maybeGracefulHandover()
	r.maybeGracefulHandover()
	if r.leadTransferee != None {
		t.Fatalf("leadTransferee = %d: the critical window must restart after recovery", r.leadTransferee)
	}
}
