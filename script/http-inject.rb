#!/usr/bin/env ruby

require 'ritm'
require 'json'
require 'set'

reqs = Set[]

Ritm.on_response do |req,resp|
  uri = req.request_uri
  reqs.add(uri)

  res = JSON.parse(resp.body)
  res['some_new_field'] = 5
  resp.body = JSON.generate(res)
end

Ritm.start
puts 'listening.. press enter to quit'
gets
Ritm.shutdown

puts 'shutting down. intercepted following set of endpoints:'
puts reqs.to_a().map{ |uri| uri.request_uri }.sort
