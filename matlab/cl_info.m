function [stats] = cl_info( C )
%VISUALIZE_CLUSTERING Summary of this function goes here
%   Detailed explanation goes here

minv = Inf;
mean = 0;
median = 0;
maxv = 0;
n = 0;

for i = 1:length(C)
    s = length(C{i});
    minv = min(minv,s);
    maxv = max(maxv,s);
    mean = mean + s;
    n = n + 1;
end

mean = mean / n;

stats = [ minv mean median maxv n];
end
