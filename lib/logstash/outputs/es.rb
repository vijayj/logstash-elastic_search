# encoding: utf-8
require "logstash/namespace"
require "logstash/outputs/base"
require 'elasticsearch'

#for debugging
require "ap"


# This output lets you store and update logs in elasticsearch.
#
class LogStash::Outputs::Es < LogStash::Outputs::Base

  #TODO(VJ) - handle gem dependency etc, handle templates for ES, authentication etc, buffering etc

  config_name "es"
  milestone 1

  # The hostname or ip address to reach your elasticsearch server.
  config :host, :validate => :string, :required => true

  # The port for ElasticSearch HTTP interface to use.
  config :port, :validate => :number, :default => 9200

  # The document ID for the index. Useful for overwriting existing entries in
  # elasticsearch with the same ID.
  config :document_id, :validate => :string, :default => nil

  config :index, :validate => :string, :default => "logstash-%{+YYYY.MM.dd}"

  # The index type to write events to. Generally you should try to write only
  # similar events to the same 'type'. String expansion '%{foo}' works here.
  config :index_type, :validate => :string

  # Enable debugging. Tries to pretty-print the entire event object.
  config :debug, :validate => :boolean, :default => false


  attr_reader :client


  public
  def register
    @client = Elasticsearch::Client.new log: @debug

    @client.transport.reload_connections!

    @client.cluster.health

    #add templates etc
  end

  public
  def receive(event)

      index = event.sprintf(@index)

      # Set the 'type' value for the index.
      if @index_type.nil?
        type =  event["type"] || "logs"
      else
        type = event.sprintf(@index_type)
      end

      if @debug
        puts event.to_hash.awesome_inspect + "\n"
      end

      update  = event.remove("UPDATE")
      if update
        p "updating"  if @debug
        #client.update :index => index, :type => type , :id => event.sprintf(@document_id), :body => { script: 'ctx._source.tags += tag', params: { tag: 'x' } }
        client.update :index => index, :type => type , :id => event.sprintf(@document_id), :body => { doc: event.to_hash }
      else
        p "updating"  if @debug
        client.index  index: index, type: type, id: event.sprintf(@document_id), body: event.to_hash
      end
  end # def receive

end
