#!/usr/bin/env octave -q
# -*- octave -*-

x = load("oneday.dat");
[s, h] = hist(x(:,2), 100);
M = [h' s'];
save "oneday.hist" M
