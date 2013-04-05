#
# Cookbook Name:: mongodb
# Recipe:: default
#
# Copyright 2011, edelight GmbH
# Authors:
#       Markus Korn <markus.korn@edelight.de>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

package "mongodb" do
  action :install
end

needs_mongo_gem = (node.recipes.include?("mongodb::replicaset") or node.recipes.include?("mongodb::mongos"))

template "/etc/mongodb.conf" do
  source "mongodb.config.erb"
  group node['mongodb']['root_group']
  owner "root"
  mode 0644
  variables(
    :dbpath    => node[:mongodb][:dbpath],
    :logpath   => node[:mongodb][:logpath],
    :ipaddress  => node[:mongodb][:ipaddress],
    :port       => node[:mongodb][:port],
    :logappend => node[:mongodb][:logappend],
    :configdb => node[:mongodb][:configserver],
    :replicaset_name => node[:mongodb][:replicaset_name],
    :configsrv => node[:mongodb][:configsrv],
    :shardsrv => node[:mongodb][:shardsrv],
    :enable_rest => node[:mongodb][:enable_rest]
  )
end

template "/etc/init/mongodb.conf" do
  source "mongodb.upstart.erb"
  group node['mongodb']['root_group']
  owner "root"
  mode 0644
  variables(
    :dbpath    => node[:mongodb][:dbpath],
    :logpath   => node[:mongodb][:logpath]
  )
end

bash "Stopping MongoDB since the service doesn't stop correctly" do
  code <<-BASH_SCRIPT
  /usr/bin/mongod --shutdown --dbpath #{node[:mongodb][:dbpath]}
  rm -f #{node[:mongodb][:dbpath]}/mongod.lock
  rm -f /etc/init.d/mongodb && ln -s /lib/init/upstart-job /etc/init.d/mongodb
  BASH_SCRIPT
end


if needs_mongo_gem
  # install the mongo ruby gem at compile time to make it globally available
  gem_package 'mongo' do
    action :nothing
  end.run_action(:install)
  Gem.clear_paths
end

if node.recipes.include?("mongodb::default") or node.recipes.include?("mongodb")
  # configure default instance
  mongodb_instance "mongodb" do
    mongodb_type "mongod"
    port         node['mongodb']['port']
    logpath      node['mongodb']['logpath']
    dbpath       node['mongodb']['dbpath']
    enable_rest  node['mongodb']['enable_rest']
  end
end

bash "Replacing /etc/init.d/mongodb with upstart-job hook" do
  code <<-BASH_SCRIPT
  rm -f /etc/init.d/mongodb && ln -s /lib/init/upstart-job /etc/init.d/mongodb
  BASH_SCRIPT
end
