# veelobot
my www crawler

needs
Net::Curl::Easy
Net::Curl::Multi
Compress::LZ4


libcurl
c-ares

without c-ares it will be slow
async dns is what we want



sometimes is segfaults/bus error with high number of curl handles, not sure why
