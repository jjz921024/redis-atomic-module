NAME := atomic-module
VER := 1.0.0
OBJECTS := $(patsubst %.c,%.o,$(wildcard src/*.c))

CC := gcc
CFLAGS := -W -Wall -std=gnu11 -march=native -O3 -g -ggdb -DREDISMODULE_EXPERIMENTAL_API -fPIC -fvisibility=hidden
CFLAGS += -Wno-strict-aliasing -Wno-typedef-redefinition -Wno-sign-compare -Wno-unused-parameter -Wno-unused-variable -Wno-stringop-truncation -Wno-implicit-function-declaration -Wno-int-conversion
LDFLAGS := -Wl,--allow-multiple-definition

# make target VERBOSE=1
ifeq ($(VERBOSE),)
.SILENT:
endif

.PHONE: build clean dist

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
	tar -cvzf $(NAME)-$(VER).tar.gz --exclude=.git --exclude=.vscode --exclude=build *
