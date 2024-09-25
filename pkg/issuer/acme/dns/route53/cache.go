package route53

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	route53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	logf "github.com/cert-manager/cert-manager/pkg/logs"
	"github.com/go-logr/logr"
)

var ChangeStatusCache = make(chan string)
var rClient *route53.Client
var logger logr.Logger

func AssignRoute53Client(r *route53.Client) {
	rClient = r
}

func SpanChangeMonitorWorkers(numWorkers int, closeChan chan struct{}) {
	logger.V(logf.DebugLevel).Info("queued all workers...")
	quitAll := make([]chan struct{}, numWorkers)
	for i := range numWorkers {
		go func(quit chan struct{}) {
			for {
				select {
				case statusID := <-ChangeStatusCache:
					requeue := GetChange(statusID)
					if requeue {
						ChangeStatusCache <- statusID
					}
				case <-quit:
					return
				}
			}
		}(quitAll[i])

		logger.V(logf.DebugLevel).Info("queued all workers...")
	}

	select {
	case <-closeChan:
		for i := range numWorkers {
			quitAll[i] <- struct{}{}
		}
	}
}

func GetChange(statusID string) (requeue bool) {
	reqParams := &route53.GetChangeInput{
		Id: aws.String(statusID),
	}
	resp, err := rClient.GetChange(context.TODO(), reqParams)
	if err != nil {
		logger.V(logf.DebugLevel).WithValues("error", err).Info("ignoring error")
		return false
	}

	if resp.ChangeInfo.Status != route53types.ChangeStatusInsync {
		logger.V(logf.DebugLevel).Info(fmt.Sprintf("[GetChange] NOT INSYNC : %q, %s", resp, statusID))
		return true
	}
	return false
}

func init() {
	logger = logf.FromContext(context.TODO(), "extra-debugger")
	go SpanChangeMonitorWorkers(25, make(chan struct{}))
}
