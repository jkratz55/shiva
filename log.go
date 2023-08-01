//   Copyright 2023 Joseph Kratz
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package shiva

import (
	"go.uber.org/zap"
)

type Logger interface {
	Debug(msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

type logger struct {
	sl *zap.SugaredLogger
}

func (l logger) Debug(msg string, keyAndValues ...any) {
	l.sl.Debugw(msg, keyAndValues...)
}

func (l logger) Info(msg string, keyAndValues ...any) {
	l.sl.Infow(msg, keyAndValues...)
}

func (l logger) Warn(msg string, keyAndValues ...any) {
	l.sl.Warnw(msg, keyAndValues...)
}

func (l logger) Error(msg string, keyAndValues ...any) {
	l.sl.Errorw(msg, keyAndValues...)
}

func newLogger() (*logger, error) {
	return nil, nil
}
