package state

import (
	"fmt"

	"github.com/hashicorp/nomad/nomad/stream"
	"github.com/hashicorp/nomad/nomad/structs"
)

const (
	TopicDeployment stream.Topic = "Deployment"
)

type DeploymentEvent struct {
	Event      string
	Deployment *structs.Deployment
	Job        *structs.Job          `json:",omitempty"`
	Eval       *structs.Evaluation   `json:",omitempty"`
	Allocs     []*structs.Allocation `json:",omitempty"`
}

func DeploymentEventFromChanges(msgType structs.MessageType, tx ReadTxn, changes Changes) ([]stream.Event, error) {

	var deployment *structs.Deployment
	var job *structs.Job
	var eval *structs.Evaluation
	var allocs []*structs.Allocation

	var eventType string
	switch msgType {
	case structs.DeploymentStatusUpdateRequestType:
		eventType = "StatusUpdate"
	case structs.DeploymentPromoteRequestType:
		eventType = "Promotion"
	case structs.DeploymentAllocHealthRequestType:
		eventType = "AllocHealth"
	}

	for _, change := range changes.Changes {
		switch change.Table {
		case "deployment":
			after, ok := change.After.(*structs.Deployment)
			if !ok {
				return nil, fmt.Errorf("transaction change was not a Deployment")
			}

			deployment = after
		case "jobs":
			after, ok := change.After.(*structs.Job)
			if !ok {
				return nil, fmt.Errorf("transaction change was not a Job")
			}

			job = after
		case "evals":
			after, ok := change.After.(*structs.Evaluation)
			if !ok {
				return nil, fmt.Errorf("transaction change was not an Evaluation")
			}

			eval = after
		case "allocs":
			after, ok := change.After.(*structs.Allocation)
			if !ok {
				return nil, fmt.Errorf("transaction change was not an Allocation")
			}

			// copy alloc and remove the nested job since it will be
			// included in the event payload
			a := after.Copy()
			a.Job = nil

			allocs = append(allocs, a)
		}
	}

	// grab latest job if it was not touched
	if job == nil {
		j, err := tx.First("jobs", "id", deployment.Namespace, deployment.JobID)
		if err != nil {
			return nil, fmt.Errorf("retrieving job for deployment event: %w", err)
		}
		if j != nil {
			job = j.(*structs.Job)
		}
	}

	event := stream.Event{
		Topic: TopicDeployment,
		Index: changes.Index,
		Key:   deployment.ID,
		Payload: &DeploymentEvent{
			Event:      eventType,
			Deployment: deployment,
			Job:        job,
			Eval:       eval,
			Allocs:     allocs,
		},
	}

	return []stream.Event{event}, nil
}
