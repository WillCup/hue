// Licensed to Cloudera, Inc. under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  Cloudera, Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var AssistDbSource = (function () {

  var sortFunctions = {
    alpha: function (a, b) {
      return a.catalogEntry.name.localeCompare(b.catalogEntry.name);
    },
    creation: function (a, b) {
      if (a.catalogEntry.isField()) {
        return a.catalogEntry.getIndex() - b.catalogEntry.getIndex();
      }
      return sortFunctions.alpha(a, b);
    },
    popular: function (a, b) {
      if (a.popularity() === b.popularity()) {
        return sortFunctions.creation(a, b);
      }
      return b.popularity() - a.popularity();
    }
  };

  /**
   * @param {Object} options
   * @param {Object} options.i18n
   * @param {string} options.type
   * @param {string} options.name
   * @param {Object} options.navigationSettings
   * @constructor
   */
  function AssistDbSource (options) {

    var self = this;
    self.isSource = true;
    self.i18n = options.i18n;
    self.navigationSettings = options.navigationSettings;
    self.apiHelper = ApiHelper.getInstance();
    self.sourceType = options.type;
    self.name = options.name;

    self.hasErrors = ko.observable(false);
    self.simpleStyles = ko.observable(false);
    self.isSearchVisible = ko.observable(false);
    self.sortFunctions = sortFunctions;

    self.highlight = ko.observable(false);

    self.invalidateOnRefresh = ko.observable('cache');

    self.activeSort = ko.observable('creation');

    self.activeSort.subscribe(function (newSort) {
      if (newSort === 'popular') {
        // TODO: Sort popular databases
        self.databases.sort(sortFunctions.alpha)
      } else {
        self.databases.sort(sortFunctions[newSort]);
      }
    });

    self.filter = {
      querySpec: ko.observable({})
    };

    self.filterActive = ko.pureComputed(function () {
      return self.filter.querySpec() && self.filter.querySpec().query !== '';
    });

    var storageSearchVisible = $.totalStorage(self.sourceType + ".assist.searchVisible");
    self.searchVisible = ko.observable(storageSearchVisible || false);

    self.searchVisible.subscribe(function (newValue) {
      $.totalStorage(self.sourceType + ".assist.searchVisible", newValue);
    });

    self.databases = ko.observableArray();

    self.hasEntries = ko.pureComputed(function() {
      return self.databases().length > 0;
    });

    self.filteredEntries = ko.pureComputed(function () {
      if (!self.filter.querySpec() || typeof self.filter.querySpec().query === 'undefined' || !self.filter.querySpec().query) {
        return self.databases();
      }
      var result = [];
      $.each(self.databases(), function (index, database) {
        if (database.catalogEntry.name.toLowerCase().indexOf(self.filter.querySpec().query.toLowerCase()) > -1) {
          result.push(database);
        }
      });
      return result;
    });

    self.autocompleteFromEntries = function (nonPartial, partial) {
      var result = [];
      var partialLower = partial.toLowerCase();
      self.databases().forEach(function (db) {
        if (db.catalogEntry.name.toLowerCase().indexOf(partialLower) === 0) {
          result.push(nonPartial + partial + db.catalogEntry.name.substring(partial.length))
        }
      });
      return result;
    };

    self.selectedDatabase = ko.observable();

    self.selectedDatabase.subscribe(function () {
      var db = self.selectedDatabase();
      if (HAS_OPTIMIZER && db && !db.popularityIndexSet && self.sourceType !== 'solr') {
        db.catalogEntry.loadNavOptMetaForChildren({ silenceErrors: true }).done(function () {
          var applyPopularity = function () {
            db.entries().forEach(function (entry) {
              if (entry.catalogEntry.navOptMeta && entry.catalogEntry.navOptMeta.popularity >= 5) {
                entry.popularity(entry.catalogEntry.navOptMeta.popularity )
              }
            });
            if (self.activeSort() === 'popular') {
              db.entries.sort(sortFunctions.popular);
            }
          };

          if (db.loading()) {
            var subscription = db.loading.subscribe(function () {
              subscription.dispose();
              applyPopularity();
            });
          } else if (db.entries().length == 0) {
            var subscription = db.entries.subscribe(function (newEntries) {
              if (newEntries.length > 0) {
                subscription.dispose();
                applyPopularity();
              }
            });
          } else {
            applyPopularity();
          }
        });
      }
    });

    huePubSub.subscribe('assist.database.get', function (callback) {
      callback(self.selectedDatabase());
    });

    self.reloading = ko.observable(false);

    self.loadingTables = ko.pureComputed(function() {
      return typeof self.selectedDatabase() != "undefined" && self.selectedDatabase() !== null && self.selectedDatabase().loading();
    });

    self.loadingSamples = ko.observable(true);
    self.samples = ko.observable();

    self.selectedDatabaseChanged = function () {
      if (self.selectedDatabase()) {
        if (!self.selectedDatabase().hasEntries() && !self.selectedDatabase().loading()) {
          self.selectedDatabase().loadEntries()
        }
        self.apiHelper.setInTotalStorage('assist_' + self.sourceType, 'lastSelectedDb', self.selectedDatabase().catalogEntry.name)
        huePubSub.publish("assist.database.set", {
          source: self.sourceType,
          name: self.selectedDatabase().catalogEntry.name
        })
      }
    };

    self.loaded = ko.observable(false);
    self.loading = ko.observable(false);
    var dbIndex = {};
    var nestedFilter = {
      querySpec: ko.observable({}),
      showTables: ko.observable(true),
      showViews: ko.observable(true),
      activeEditorTables: ko.observableArray([])
    };

    huePubSub.subscribe('editor.active.locations', function (activeLocations) {
      var activeTables = [];
      // TODO: Test multiple snippets
      if (self.sourceType !== activeLocations.type) {
        return;
      }
      activeLocations.locations.forEach(function (location) {
        if (location.type === 'table') {
          activeTables.push(location.identifierChain.length == 2 ? { table: location.identifierChain[1].name, db: location.identifierChain[0].name} : { table: location.identifierChain[0].name });
        }
      });
      nestedFilter.activeEditorTables(activeTables);
    });

    var updateDatabases = function (names, lastSelectedDb) {
      dbIndex = {};
      var hasNavMeta = false;
      var dbPromises = [];
      var dbs = [];

      names.forEach(function (name) {
        dbPromises.push(DataCatalog.getEntry({ sourceType: self.sourceType, path: name, definition: { type: 'database' }}).done( function (catalogEntry) {
          hasNavMeta = hasNavMeta || !!catalogEntry.navigatorMeta;
          var database = new AssistDbEntry(catalogEntry, null, self, nestedFilter, self.i18n, self.navigationSettings);
          dbIndex[name] = database;
          if (name === lastSelectedDb) {
            self.selectedDatabase(database);
            self.selectedDatabaseChanged();
          }
          dbs.push(database);
        }));
      });

      $.when.apply($, dbPromises).always(function () {
        if (!hasNavMeta) {
          if (self.sourceType !== 'solr') {
            DataCatalog.getEntry({ sourceType: self.sourceType, path: [], definition: { type: 'source' } })
              .done(function (catalogEntry) { catalogEntry.loadNavigatorMetaForChildren({ silenceErrors: true }) });
          }
        }
        dbs.sort(sortFunctions[self.activeSort()]);
        self.databases(dbs);
        self.reloading(false);
        self.loading(false);
        self.loaded(true);
      });
    };

    self.setDatabase = function (databaseName) {
      if (databaseName && self.selectedDatabase() && databaseName === self.selectedDatabase().catalogEntry.name) {
        return;
      }
      if (databaseName && dbIndex[databaseName]) {
        self.selectedDatabase(dbIndex[databaseName]);
        self.selectedDatabaseChanged();
        return;
      }
      var lastSelectedDb = self.apiHelper.getFromTotalStorage('assist_' + self.sourceType, 'lastSelectedDb', 'default');
      if (lastSelectedDb && dbIndex[lastSelectedDb]) {
        self.selectedDatabase(dbIndex[lastSelectedDb]);
        self.selectedDatabaseChanged();
      } else if (self.databases().length > 0) {
        self.selectedDatabase(self.databases()[0]);
        self.selectedDatabaseChanged();
      }
    };

    self.initDatabases = function (callback) {
      if (self.loading()) {
        return;
      }
      self.loading(true);
      var lastSelectedDb = self.selectedDatabase() ? self.selectedDatabase().catalogEntry.name : null;
      self.selectedDatabase(null);
      self.databases([]);
      self.apiHelper.loadDatabases({
        sourceType: self.sourceType,
        successCallback: function(data) {
          self.hasErrors(false);
          updateDatabases(data, lastSelectedDb);
          if (typeof callback === 'function') {
            callback();
          }
        },
        errorCallback: function() {
          self.hasErrors(true);
          updateDatabases([]);
        },
        database: lastSelectedDb
      });
    };

    self.modalItem = ko.observable();

    self.repositionActions = function(data, event) {
      var $container = $(event.target);
      $container.find(".assist-actions, .assist-db-header-actions").css('right', -$container.scrollLeft() + 'px');
    };

    self.reload = function(allCacheTypes) {
      if (self.invalidateOnRefresh() !== 'cache') {
        huePubSub.publish('assist.invalidate.impala', {
          flush: self.invalidateOnRefresh() === 'invalidateAndFlush',
          database: self.selectedDatabase() ? self.selectedDatabase().catalogEntry.name : null
        });
      }

      self.reloading(true);
      huePubSub.publish('assist.clear.db.cache', {
        sourceType: self.sourceType,
        clearAll: true
      });
      if (allCacheTypes) {
        huePubSub.publish('assist.clear.db.cache', {
          sourceType: self.sourceType,
          cacheType: 'optimizer',
          clearAll: true
        });
      }
      self.invalidateOnRefresh('cache');
      self.initDatabases();
    };

    huePubSub.subscribe('assist.invalidate.on.refresh', function () {
      self.invalidateOnRefresh('invalidate');
    });

    huePubSub.subscribe('assist.db.refresh', function (options) {
      if (typeof options.sourceTypes === 'undefined' || options.sourceTypes.indexOf(self.sourceType) !== -1) {
        window.setTimeout(function () {
          self.reload(options.allCacheTypes);
        }, 0);
      }
    });
  }

  AssistDbSource.prototype.highlightInside = function (path) {
    var self = this;

    var foundDb;
    var index;

    var findDatabase = function () {
      $.each(self.databases(), function (idx, db) {
        db.highlight(false);
        if (db.databaseName === path[0]) {
          foundDb = db;
          index = idx;
        }
      });

      if (foundDb) {
        var whenLoaded = function () {
          if (self.selectedDatabase() !== foundDb) {
            self.selectedDatabase(foundDb);
          }
          if (!foundDb.open()) {
            foundDb.open(true);
          }
          window.setTimeout(function () {
            huePubSub.subscribeOnce('assist.db.scrollToComplete', function () {
              foundDb.highlight(true);
              // Timeout is for animation effect
              window.setTimeout(function () {
                foundDb.highlight(false);
              }, 1800);
            });
            if (path.length > 1) {
              foundDb.highlightInside(path.slice(1), []);
            } else {
              huePubSub.publish('assist.db.scrollTo', foundDb);
            }
          }, 0);
        };

        if (foundDb.hasEntries()) {
          whenLoaded();
        } else {
          foundDb.loadEntries(whenLoaded);
        }
      }
    };

    if (!self.loaded()) {
      self.initDatabases(findDatabase);
    } else {
      findDatabase();
    }
  };

  AssistDbSource.prototype.toggleSearch = function () {
    var self = this;
    self.isSearchVisible(!self.isSearchVisible());
  };

  AssistDbSource.prototype.triggerRefresh = function (data, event) {
    var self = this;
    huePubSub.publish('assist.db.refresh', { sourceTypes: [self.sourceType], allCacheTypes: event.shiftKey });
  };

  return AssistDbSource;
})();
