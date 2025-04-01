# Universal Contiki-NG Makefile for compiling projects in different directories
# Place this Makefile in your base directory

# Default values
TARGET ?= nrf52840
BOARD ?= dongle
PORT ?= /dev/ttyACM0
SRC_DIR ?= .

# Find the base directory where this Makefile is located
BASE_DIR := $(shell pwd)

# Path to Contiki-NG (update this to your actual path)
CONTIKI ?= ~/Desktop/contiki-ng

# Check if source directory exists
ifneq ($(shell test -d $(SRC_DIR) && echo yes),yes)
$(error Source directory $(SRC_DIR) does not exist)
endif

# Count C files in the source directory
C_FILES_COUNT := $(shell find $(SRC_DIR) -maxdepth 1 -name "*.c" | wc -l)
ifeq ($(C_FILES_COUNT),0)
$(error No C files found in $(SRC_DIR))
endif

# Main project logic
build:
	@echo "Building project in $(SRC_DIR)"
	@mkdir -p $(SRC_DIR)/build
	@# Create a temporary Makefile in the source directory
	@echo "CONTIKI_PROJECT = $(shell basename $$(find $(SRC_DIR) -maxdepth 1 -name "*.c" | head -1 | sed 's/\.c$$//g'))" > $(SRC_DIR)/build/Makefile
	@echo "all: \$$(CONTIKI_PROJECT)" >> $(SRC_DIR)/build/Makefile
	@echo "CONTIKI = $(CONTIKI)" >> $(SRC_DIR)/build/Makefile
	@echo "include \$$(CONTIKI)/Makefile.include" >> $(SRC_DIR)/build/Makefile
	@# Copy source files to the build directory
	@cp $(SRC_DIR)/*.c $(SRC_DIR)/build/
	@cp $(SRC_DIR)/*.h $(SRC_DIR)/build/ 2>/dev/null || true
	@# Run make in the build directory
	@cd $(SRC_DIR)/build && make TARGET=$(TARGET) BOARD=$(BOARD)
	@# Copy the compiled files back to the source directory
	@cp $(SRC_DIR)/build/*.$(TARGET) $(SRC_DIR)/ 2>/dev/null || true
	@echo "Build complete. Output files copied to $(SRC_DIR)"

flash:
	@echo "Flashing project in $(SRC_DIR)"
	@# Must build first
	@$(MAKE) -f $(BASE_DIR)/Makefile build SRC_DIR=$(SRC_DIR) TARGET=$(TARGET) BOARD=$(BOARD)
	@# Get project name
	@PROJECT_NAME=$$(basename $$(find $(SRC_DIR) -maxdepth 1 -name "*.c" | head -1 | sed 's/\.c$$//g')); \
	cd $(SRC_DIR)/build && make TARGET=$(TARGET) BOARD=$(BOARD) $$PROJECT_NAME.dfu-upload PORT=$(PORT)

clean:
	@echo "Cleaning project in $(SRC_DIR)"
	@rm -rf $(SRC_DIR)/build
	@rm -f $(SRC_DIR)/*.$(TARGET) $(SRC_DIR)/*.hex $(SRC_DIR)/*.elf $(SRC_DIR)/*.bin $(SRC_DIR)/*.dfu

# Default target
.PHONY: build flash clean
all: build
