var Instance = Backbone.Model.extend({
  name: "shard"
});
var instance = new Instance({name: "shard"});

var Queue = Backbone.View.extend({
  id: "queue",

  initialize: function() {
    this.listenTo(this.model, "change", this.render);
  },

  render: function() {
    // this.el.innerHTML = _.reduce(sizes, function(memo, ){ return memo + parseInt(num); }, 0);
    this.el.innerHTML = this.model.get("size") ?? 0;
    return this;
  },
});

var Prequeue = Backbone.View.extend({
  id: "prequeue",

  initialize: function() {
    this.listenTo(this.model, "change", this.render);
  },

  render: function() {
    var queue = this.model.get("operationQueue");
    if (queue != null) {
      var provisions = queue.provisions;
      size = _.reduce(provisions, function(memo, provision) {
        return _.reduce(provisions.internalSizes, function(memo, num) { return memo + parseInt(num); }, 0);
      }, 0);
      this.el.innerHTML = size;
    }
    return this;
  },
});

var Latency = Backbone.View.extend({
  id: "latency",

  initialize: function() {
    this.listenTo(this.model, "change", this.render);
  },

  render: function() {
    this.el.innerHTML = this.model.get("latency");
    return this;
  },
});

var Dispatched = Backbone.View.extend({
  id: "dispatched",

  initialize: function() {
    this.listenTo(this.model, "change", this.render);
  },

  render: function() {
    this.el.innerHTML = this.model.get("dispatchedSize") ?? 0;
    return this;
  },
});

var Workers = Backbone.View.extend({
  id: "workers",

  initialize: function() {
    this.listenTo(this.model, "change", this.render);
  },

  render: function() {
    this.el.innerHTML = this.model.get("activeWorkers");
    return this;
  }
});

var Backplane = Backbone.View.extend({
  tagName: "ul",

  initialize: function(latency) {
    this.prequeue = new Prequeue({ model: this.model })
    this.queue = new Queue({ model: this.model })
    this.dispatched = new Dispatched({ model: this.model })
    this.workers = new Workers({ model: this.model })
    this.latency = new Latency({ model: this.model.get("latency") })
    this.render()
  },

  render: function() {
    this.el.innerHTML = "";
    this.el.append(this.prequeue.render().el);
    this.el.append(this.queue.render().el);
    this.el.append(this.dispatched.render().el);
    this.el.append(this.workers.render().el);
    this.el.append(this.latency.render().el);
    return this
  },

  idle: function() {
    var self = this;
    var latency = self.model.get("latency");
    latency.set("start", _.now());
    self.model.fetch({
      success: function(model, response, objects) {
        latency.set("latency", _.now() - latency.get("start"));
        var status = _.bind(self.idle, self);
        _.delay(status, 17)
      },
      error: function(xhr, textStatus, errorThrown) {
        console.log({xhr: xhr, textStatus: textStatus, errorThrown: errorThrown});
        /*
        if (errorThrown.errorThrown == "timeout") {
          self.idle()
        }
        */
      },
      timeout: 10000,
    });
  }
});

var Capabilities = Backbone.Model.extend({
  url: function() {
    return "/v2/" + instance.name + "/capabilities";
  }
});

var BackplaneStatus = Backbone.Model.extend({
  xparse: function(resp, options) {
    return BackplaneStatus.deserializeBinary(resp).toObject();
  },
  url: function() {
    return "/v1test/" + instance.name + "/operation:status";
  },
});

var capabilities = new Capabilities({instance: instance});
var backplaneStatus = new BackplaneStatus({instance: instance});
window.onload = function() {
  latency = new Backbone.Model()
  backplaneStatus.set("latency", latency)
  window.backplane = new Backplane({model: backplaneStatus, el: document.getElementById("main")});

  capabilities.fetch({
    /*
    success: function(model, response, objects) {
      console.log(model.toJSON());
    },
    */
    error: function(xhr, textStatus, errorThrown) {
      console.log({xhr: xhr, textStatus: textStatus, errorThrown: errorThrown});
    },
  });

  window.backplane.idle();
};
