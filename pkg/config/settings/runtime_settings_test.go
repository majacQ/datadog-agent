// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package settings

import (
	"strings"
	"testing"

	"github.com/DataDog/datadog-agent/pkg/config"

	"github.com/stretchr/testify/assert"
)

type runtimeTestSetting struct {
	value int
}

func (t *runtimeTestSetting) Name() string {
	return "name"
}

func (t *runtimeTestSetting) Description() string {
	return "desc"
}

func (t *runtimeTestSetting) Get() (interface{}, error) {
	return t.value, nil
}

func (t *runtimeTestSetting) Set(v interface{}) error {
	t.value = v.(int)
	return nil
}

func (t *runtimeTestSetting) Hidden() bool {
	return false
}

func setupConf() config.Config {
	conf := config.NewConfig("datadog", "DD", strings.NewReplacer(".", "_"))
	config.InitConfig(conf)
	return conf
}

func cleanRuntimeSetting() {
	runtimeSettings = make(map[string]RuntimeSetting)
}

func TestRuntimeSettings(t *testing.T) {
	cleanRuntimeSetting()
	runtimeSetting := runtimeTestSetting{1}

	err := RegisterRuntimeSetting(&runtimeSetting)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(RuntimeSettings()))

	v, err := GetRuntimeSetting(runtimeSetting.Name())
	assert.Nil(t, err)
	assert.Equal(t, runtimeSetting.value, v)

	err = SetRuntimeSetting(runtimeSetting.Name(), 123)
	assert.Nil(t, err)

	v, err = GetRuntimeSetting(runtimeSetting.Name())
	assert.Nil(t, err)
	assert.Equal(t, 123, v)

	err = RegisterRuntimeSetting(&runtimeSetting)
	assert.NotNil(t, err)
	assert.Equal(t, "duplicated settings detected", err.Error())
}

func TestLogLevel(t *testing.T) {
	cleanRuntimeSetting()
	config.SetupLogger("TEST", "debug", "", "", true, true, true)

	ll := LogLevelRuntimeSetting{}
	assert.Equal(t, "log_level", ll.Name())

	err := ll.Set("off")
	assert.Nil(t, err)

	v, err := ll.Get()
	assert.Equal(t, "off", v)
	assert.Nil(t, err)

	err = ll.Set("WARNING")
	assert.Nil(t, err)

	v, err = ll.Get()
	assert.Equal(t, "warn", v)
	assert.Nil(t, err)

	err = ll.Set("invalid")
	assert.NotNil(t, err)
	assert.Equal(t, "unknown log level: invalid", err.Error())

	v, err = ll.Get()
	assert.Equal(t, "warn", v)
	assert.Nil(t, err)
}

func TestProfiling(t *testing.T) {
	cleanRuntimeSetting()
	setupConf()

	ll := ProfilingRuntimeSetting("profiling")
	assert.Equal(t, "profiling", ll.Name())

	err := ll.Set("false")
	assert.Nil(t, err)

	v, err := ll.Get()
	assert.Equal(t, false, v)
	assert.Nil(t, err)

	err = ll.Set("on")
	assert.NotNil(t, err)
}
