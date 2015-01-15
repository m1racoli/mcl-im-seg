function [ M ] = cl_spat( C, dims )
%CL_SPAT generate spatial dimentions of clustering
%   Detailed explanation goes here

l = length(C);
max_ind = 0;

for i = 1:l
	max_ind = max(max_ind,max(C{i}));
end

%disp(max_ind);

if (max_ind + 1 != prod(dims))
	error('clustering indecies dont match dimonsions');
endif

M = zeros(max_ind,1,'int32');

for i = 1:l
	c = C{i};
	for j = 1:length(c)
		M(c(j) + 1) = i;
	end
end

M = reshape(M,dims);

end
