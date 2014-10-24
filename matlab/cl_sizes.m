function v = cl_sizes( C )

l = length(C);
v = zeros(l,1);

for i = 1:l
	v(i) = length(C{i});
end

end