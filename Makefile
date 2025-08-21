NAME := atomic-module
VER := 1.0.0
OBJECTS := $(patsubst %.c,%.o,$(wildcard src/*.c))

CC := gcc
CFLAGS := -W -Wall -g -ggdb -std=gnu99 -O2 -fPIC -fvisibility=hidden
CFLAGS += -Wno-strict-aliasing -Wno-typedef-redefinition -Wno-sign-compare -Wno-unused-parameter -Wno-unused-variable -Wno-stringop-truncation -Wno-implicit-function-declaration -Wno-int-conversion
LDFLAGS := -Wl,--allow-multiple-definition

# make target VERBOSE=1
ifeq ($(VERBOSE),)
.SILENT:
endif

.PHONE: build clean dist test

build: $(OBJECTS)
	echo "LD $(NAME).so"
	$(CC) $(LDFLAGS) -shared $^ -o $(NAME).so

%.o: %.c
	echo "CC $<"
	@$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(OBJECTS)
	rm -rf $(NAME).so

# dist from source
dist: clean
	tar -cvzf $(NAME)-$(VER).tar.gz --exclude=.git --exclude=.vscode --exclude=build --exclude=img *

# 未定义时才赋值
# make test REDIS_PATH=../redis
REDIS_PATH ?= "../redis"
MODULE_PATH := $(shell pwd)

test: build
	echo "redis path: " $(REDIS_PATH)
	sed -e "s#module_path#$(MODULE_PATH)#g" tests/atomic.tcl > $(REDIS_PATH)/tests/unit/type/atomic.tcl
	sed -i 's#unit/type/string#unit/type/atomic#g' $(REDIS_PATH)/tests/test_helper.tcl
	@(cd $(REDIS_PATH); ./runtest --stack-logging --single unit/type/atomic)
	