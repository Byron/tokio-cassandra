SYNTECT_REPO = .syntect
ASSETS_SRC = .assets_src
PACKS_DIR = packs

help:
	$(info Available Targets)
	$(info ---------------------------------------------------------------------------------------------------------------)
	$(info assets           | build syntect assets)
	$(info clean            | remove intermediate directories)
	
$(SYNTECT_REPO):
	git clone https://github.com/trishume/syntect $@
	cd $@ && git submodule update --init --recursive
	
$(ASSETS_SRC): $(SYNTECT_REPO)
	@rm -Rf $@ && mkdir $@
	mkdir $@/syntax && cp -R $</testdata/Packages/YAML $@/syntax
	mkdir $@/theme && find $</testdata/Solarized -type f -name "Solarized*" -print0 | xargs -0 -I % cp "%" $@/theme

assets: $(ASSETS_SRC)
	@mkdir -p $(PACKS_DIR)
	cd $(SYNTECT_REPO) && cargo run --example gendata -- synpack ../$(ASSETS_SRC)/syntax ../$(PACKS_DIR)/syntax.newlines.packdump ../$(PACKS_DIR)/syntax.nonewlines.packdump 
	cd $(SYNTECT_REPO) && cargo run --example gendata -- themepack ../$(ASSETS_SRC)/theme ../$(PACKS_DIR)/themes.themedump
	
clean:
	rm -Rf $(ASSETS_SRC)
	
	
