function h = clustering_hist( C )

l = length(C);
v = zeros(l,1);

for i = 1:l
	v(i) = length(C{i});
end

h = hist(v);
drawnow();
end