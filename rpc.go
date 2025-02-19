package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"

	"golang.org/x/time/rate"
)

func rpcQuery(method string, params map[string]interface{}) (map[string]interface{}, error) {
	rpcURL := "https://rpc.unicorn.meme" // Adjust if the URL differs
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal RPC payload: %v", err)
	}
	resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("RPC request failed: %v", err)
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode RPC response: %v", err)
	}
	if errData, ok := result["error"]; ok {
		return nil, fmt.Errorf("RPC error: %v", errData)
	}
	return result["result"].(map[string]interface{}), nil
}

func fetchDelegationsViaRPC(state *State, workers int, limiter *rate.Limiter) ([]map[string]interface{}, error) {
	validators, err := fetchAll("/cosmos/staking/v1beta1/validators", "validators", state, workers, limiter)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch validators: %v", err)
	}
	var delegations []map[string]interface{}
	for _, val := range validators {
		operatorAddr := val["operator_address"].(string)
		rpcParams, err := rpcQuery("abci_query", map[string]interface{}{
			"path":   fmt.Sprintf("/custom/staking/validator/%s/delegations", operatorAddr),
			"data":   "",
			"height": fmt.Sprintf("%d", state.BlockHeight),
		})
		if err != nil {
			logger.Printf("Failed to fetch delegations for %s via RPC: %v", operatorAddr, err)
			continue
		}
		response := rpcParams["response"].(map[string]interface{})
		delegData, err := base64.StdEncoding.DecodeString(response["value"].(string))
		if err != nil {
			logger.Printf("Failed to decode delegations for %s: %v", operatorAddr, err)
			continue
		}
		var deleg []map[string]interface{}
		if err := json.Unmarshal(delegData, &deleg); err != nil {
			logger.Printf("Failed to unmarshal delegations for %s: %v", operatorAddr, err)
			continue
		}
		delegations = append(delegations, deleg...)
	}
	return delegations, nil
}
