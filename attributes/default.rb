#
# Cookbook Name:: mongodb
# Attributes:: default
#
# Copyright 2010, edelight GmbH
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

default[:mongodb][:user] = "mongodb"
default[:mongodb][:group] = "mongodb"

default[:mongodb][:dbpath]       = "/data/mongodb"
default[:mongodb][:logpath]      = "/log/mongodb"
default[:mongodb][:journal_path] = "/journal" 
default[:mongodb][:logappend] = true

default[:mongodb][:port] = 27017
default[:mongodb][:ipaddress] = "172.16.3.11"

# cluster identifier
default[:mongodb][:client_roles] = []
default[:mongodb][:cluster_name] = nil
default[:mongodb][:replicaset_name] = nil
default[:mongodb][:shard_name] = "default"

default[:mongodb][:enable_rest] = false

default[:mongodb][:number_ebs_drives] = 4
default[:mongodb][:ebs_size] = 125 #this is Gb size
default[:mongodb][:raid_config] = 10
default[:mongodb][:ebs_drive_name] = "md0"
default[:mongodb][:ebs_volume_group_name] = "vg0"

default[:mongodb][:mongodb_log] = "log"
default[:mongodb][:mongodb_data] = "data"
default[:mongodb][:mongodb_journal] = "journal"

case node['platform']
when "freebsd"
  default[:mongodb][:defaults_dir] = "/etc/rc.conf.d"
  default[:mongodb][:init_dir] = "/usr/local/etc/rc.d"
  default[:mongodb][:root_group] = "wheel"
else
  default[:mongodb][:defaults_dir] = "/etc/default"
  default[:mongodb][:init_dir] = "/etc/init.d"
  default[:mongodb][:root_group] = "root"
end
