/*
Copyright 2025 The Kubernetes Authors.
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sidecarmounter

import (
	"golang.org/x/oauth2"
)

type TokenManager interface {
	GetTokenSource(token *oauth2.Token) oauth2.TokenSource
}

type tokenManager struct{}

func NewTokenManager() TokenManager {
	return &tokenManager{}
}

func (tm *tokenManager) GetTokenSource(token *oauth2.Token) oauth2.TokenSource {
	return &TokenSource{
		token: token,
	}
}