# CDISC Pilot 01 — local data fetch
#
# Usage:
#   make data        # download SDTM + reference ADaM XPTs
#   make clean-data  # remove all downloaded inputs

PILOT_REPO  := https://raw.githubusercontent.com/cdisc-org/sdtm-adam-pilot-project/master
DATA_DIR    := data
SDTM_DIR    := $(DATA_DIR)/sdtm
REF_DIR     := $(DATA_DIR)/adam_reference
DEFINE_DIR  := $(DATA_DIR)/define

SDTM_FILES := dm.xpt ae.xpt ex.xpt lb.xpt sv.xpt vs.xpt suppdm.xpt
REF_FILES  := adsl.xpt adae.xpt adlbc.xpt

.PHONY: data sdtm reference define clean-data

data: sdtm reference define

sdtm: | $(SDTM_DIR)
	@for f in $(SDTM_FILES); do \
	  echo "fetch sdtm/$$f"; \
	  curl -fsSL -o $(SDTM_DIR)/$$f $(PILOT_REPO)/updated-pilot-submission-package/900172/m5/datasets/cdiscpilot01/tabulations/sdtm/$$f; \
	done

reference: | $(REF_DIR)
	@for f in $(REF_FILES); do \
	  echo "fetch adam_reference/$$f"; \
	  curl -fsSL -o $(REF_DIR)/$$f $(PILOT_REPO)/updated-pilot-submission-package/900172/m5/datasets/cdiscpilot01/analysis/adam/datasets/$$f; \
	done

define: | $(DEFINE_DIR)
	curl -fsSL -o $(DEFINE_DIR)/define.xml \
	  $(PILOT_REPO)/updated-pilot-submission-package/900172/m5/datasets/cdiscpilot01/analysis/adam/datasets/define.xml

$(SDTM_DIR) $(REF_DIR) $(DEFINE_DIR):
	mkdir -p $@

clean-data:
	rm -rf $(SDTM_DIR) $(REF_DIR) $(DEFINE_DIR)
