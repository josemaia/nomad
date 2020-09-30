package state

import (
	"fmt"

	"github.com/hashicorp/nomad/nomad/stream"
	"github.com/hashicorp/nomad/nomad/structs"
)

const (
	TopicEval  stream.Topic = "Eval"
	TopicAlloc stream.Topic = "Alloc"
)

type EvalEvent struct {
	Eval *structs.Evaluation
}

type AllocEvent struct {
	Alloc *structs.Allocation
}

func ApplyPlanResultEventsFromChanges(tx ReadTxn, changes Changes) ([]stream.Event, error) {
	var events []stream.Event
	for _, change := range changes.Changes {
		switch change.Table {
		case "deployment":
			after, ok := change.After.(*structs.Deployment)
			if !ok {
				return nil, fmt.Errorf("transaction change was not a Deployment")
			}

			event := stream.Event{
				Topic: TopicDeployment,
				Index: changes.Index,
				Key:   after.ID,
				Payload: &DeploymentEvent{
					Event:      "StatusUpdate",
					Deployment: after,
				},
			}
			events = append(events, event)
		case "evals":
			after, ok := change.After.(*structs.Evaluation)
			if !ok {
				return nil, fmt.Errorf("transaction change was not an Evaluation")
			}

			event := stream.Event{
				Topic: TopicEval,
				Index: changes.Index,
				Key:   after.ID,
				Payload: &EvalEvent{
					Eval: after,
				},
			}

			events = append(events, event)
		case "allocs":
			after, ok := change.After.(*structs.Allocation)
			if !ok {
				return nil, fmt.Errorf("transaction change was not an Allocation")
			}

			event := stream.Event{
				Topic: TopicAlloc,
				Index: changes.Index,
				Key:   after.ID,
				Payload: &AllocEvent{
					Alloc: after,
				},
			}

			events = append(events, event)
		}
	}

	return events, nil
}
