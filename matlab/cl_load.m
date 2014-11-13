function [ C ] = cl_load( filename )
% reads a clustering from a textfile where each line of delimited integers
% represents a cluster.
% the result is a cell array containing vectors of interger ids of
% the items in the respective clusters
fileID = fopen(filename);
lines = textscan(fileID,'%s', 'delimiter','\n');
fclose(fileID);
lines = lines{1};
l = length(lines);
fprintf("%s: %d\n",filename,l);
C = cell(l,1);
for i = 1:l
    r = textscan(lines{i},'%d');
    C{i} = r{1};
end
end

