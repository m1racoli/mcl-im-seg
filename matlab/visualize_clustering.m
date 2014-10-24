function [ C ] = visualize_clustering( clustering, dims )
%VISUALIZE_CLUSTERING Summary of this function goes here
%   Detailed explanation goes here

C = fromIndex(zeros(size(dims)),999,dims);
end

function X = fromIndex(X, idx, dims)

for i = 1:length(dims)
    X(i) = mod(idx,dims(i));
    idx = floor(idx / dims(i));
end
end
