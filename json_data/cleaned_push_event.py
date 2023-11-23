import datetime

data = {
  "id": "14686248151",
  "type": "PushEvent",
  "actor": {
    "id": 10810283,
    "login": "direwolf-github",
    "display_login": "direwolf-github",
    "gravatar_id": "",
    "url": "https://api.github.com/users/direwolf-github",
    "avatar_url": "https://avatars.githubusercontent.com/u/10810283?"
  },
  "repo": {
    "id": 325989521,
    "name": "direwolf-github/ephemeral-ci-cd9a1d94",
    "url": "https://api.github.com/repos/direwolf-github/ephemeral-ci-cd9a1d94"
  },
  "payload": {
    "push_id": 6282909278,
    "size": 1,
    "distinct_size": 1,
    "ref": "refs/heads/branch-e800d913",
    "head": "7208ffa7968dbf5a72a7316f9592f96b7a248e1d",
    "before": "ab8d37682db12d8a40fe7c1f8ecbd68eb362c515",
    "commits": [
      {
        "sha": "7208ffa7968dbf5a72a7316f9592f96b7a248e1d",
        "author": {
          "name": "direwolf-github",
          "email": "844d767d029627d30a0e42f140c1da118de43553@salesforce.com"
        },
        "message": "add readme.txt",
        "distinct": True,
        "url": "https://api.github.com/repos/direwolf-github/ephemeral-ci-cd9a1d94/commits/7208ffa7968dbf5a72a7316f9592f96b7a248e1d"
      }
    ]
  },
  "public": True,
  "created_at": "2021-01-01T14:00:00Z"
}

cleaned_data = {
    'actor__avatar_url': data['actor']['avatar_url'],
    'actor__display_login': data['actor']['display_login'],
    'actor__gravatar_id': data['actor']['gravatar_id'],
    'actor__id': data['actor']['id'],
    'actor__login': data['actor']['login'],
    'actor__url': data['actor']['url'],
    'created_at': datetime.datetime.strptime(data['created_at'], '%Y-%m-%dT%H:%M:%SZ'),
    'id': data['id'],
    'org__avatar_url': data['org']['avatar_url'],
    'org__gravatar_id': data['org']['gravatar_id'],
    'org__id': data['org']['id'],
    'org__login': data['org']['login'],
    'org__url': data['org']['url'],
    'payload__before':  data['payload']['before'],
    'payload__distinct_size':data['payload']['distinct_size'],
    'payload__head':  data['payload']['head'],
    'payload__push_id':   data['payload']['push_id'],
    'payload__ref': data['payload']['ref'],
    'payload__size' :               data['payload']['size'],
    'public'      :       data['public'],
    'repo__id'     :      data['repo']['id'] ,
    'repo__name'     :               data['repo']['name'] ,
    'repo__url'     :         data['repo']['url'] ,
    'type'       :                 data['type']

}
'search_key__event_type'  :     String,
    'search_key__gh_archive_day'  : String,
    'search_key__gh_archive_hour'  :String,
    'search_key__gh_archive_month': String,
    'search_key__gh_archive_year':  String,
    'search_key__id' :              Int64,
    'search_key__number'   :        Int64,
    'search_key__owner'  :          String,
    'search_key__repo'  :           String,
    'search_key__updated_at'   :    Int64




    `payload__commits.author` Array(String),
    `payload__commits.distinct` Array(Int64),
    `payload__commits.message` Array(String),
    `payload__commits.sha` Array(String),
    `payload__commits.url` Array(String),




